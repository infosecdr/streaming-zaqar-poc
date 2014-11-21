# Copyright 2014 Symantec.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import logging
import math
import os
import random
from kafka import KafkaClient, SimpleProducer, SimpleConsumer
import sys
import time
import struct
from kafka.common import UnknownTopicOrPartitionError
from MessageStream import MessageStream

# design notes for this are at ../docs/kafka-driver-design-notes.doc

class KafkaDriver:

    def __init__(self, driver_args, event_loop):
        self.logger = logging.getLogger('KafkaDriver') # possible TODO: get logger from invoker
        self.logger.setLevel(logging.INFO)
        console_log_handler = logging.StreamHandler(sys.stdout)
        self.logger.addHandler(console_log_handler)

        self.logger.info("KafkaDriver initialized; driver_args=%s" % (driver_args))
        self.event_loop = event_loop
        if driver_args is "":
            kafka_server_addr =  "localhost:9092"
        else:
            kafka_server_addr = driver_args
        client_id = "KafkaDriver-%d-%d" % (time.time(), os.getpid()) # generate a unique client ID so that Kafka doesn't confuse us with a different instance
        self.kafka = KafkaClient(kafka_server_addr, client_id=client_id)

        self.queue_name = None
        ## APPEND direction
        self.get_message_stream = None
        # how frequently to add check for messages and (space permitting) to add them to the GET message stream, in seconds
        self.MESSAGE_CHECK_FREQ = 0.010
        # how many message we have sent from various queues
        self.get_message_count = 0
        self.producer = None
        ## GET direction
        self.consumer = None
        self.get_message_count = 0
        self.MAX_KAFKA_REQ_BATCH_MSGS = 200 # most number of messages that we will request from Kafka at a time

    ######## APPEND direction ########

    # called to tell driver of a new stream of appends than are going to come in; these should go to the end of the named queue
    def prepare_for_append_stream(self, queue_name):
        self.logger.info("KafkaDriver prepare_for_append_stream got: queue_name=%s" % (queue_name))
        self.queue_name = str(queue_name)
        self.producer = SimpleProducer(
            self.kafka,
            async=True,
            req_acks=SimpleProducer.ACK_AFTER_LOCAL_WRITE,
            ack_timeout=5000,
            batch_send=True,
            batch_send_every_n= 100,
            batch_send_every_t=1000,
            random_start=True
        )

    def append(self, payload, ttl):
        ttl = int(ttl)
        self.logger.debug("KafkaDriver append got: ttl=%d, payload='%s'" % (ttl, payload))
        try:
            self.producer.send_messages(self.queue_name,payload)
        except UnknownTopicOrPartitionError:
            self.logger.warn("Kafka reports unknown topic or invalid partition number: " + str(sys.exc_info()))
            return 500
        except:
            self.logger.warn("Got exception from kafka-python SimpleProducer:" + str(sys.exc_info()))
            return 500

        # if random.uniform(0,1) < self.FRACTION_MSGS_TO_FAKE_APPEND_ERROR:
        #     self.logger.debug("faking error")
        #     return 400
        return 100

    def cancel_append_stream(self):
        self.logger.info("KafkaDriver cancel_append_stream got called")
        self.producer.stop()
        self.producer = None
        self.queue_name = None

    ######## GET direction ########

    # called to tell driver that a new stream of messages is needed for return to a client.   message_stream_queue is an instance of MessageStream to use to put messages the driver has available as a response to this request.  Other arguments have same meaning as in the Marconi API.
    def init_get_stream(self, get_message_stream, queue_name_spec, starting_marker, echo_requested, include_claimed):
        self.logger.info("KafkaDriver prepare_to_get_messages got: queue_name=%s, echo_requested=%s, include_claimed=%s, starting_marker=%s" % (queue_name_spec,str(echo_requested),str(include_claimed),starting_marker))
        self.logger.info("warning: KafkaDriver ignores echo_requested and include_claimed in GET requests")
        self.consume_group = "cg1" # default consume group
        if len(starting_marker) > 0:
            self.consume_group = starting_marker
        self.logger.info("consume group="+self.consume_group)

        # if the queue name contains "/n"  at the end, we interpret that is referring to partition to read from
        queue_name, partition_part = queue_name_spec.split("/",2)
        if partition_part is None:
            partition = None
        else:
            partition = int(partition_part)
            self.logger.info("limiting topic %s to partition %d" % (queue_name, partition))

        self.get_message_stream = get_message_stream
        self.queue_name = str(queue_name)
        self.consumer = SimpleConsumer(
            client=self.kafka,
            group=self.consume_group,
            topic=self.queue_name,
            partitions=[partition],
            auto_commit=False, # it seems we cannot do any kind of commit when using kafka-pythong 0.9.1 with Kafka versions before 0.8.1 because kafka-python will send a OffsetFetchReqeust (request type 9) or OffsetCommitRequest (request type 8) which is not supported
            fetch_size_bytes= self.MAX_KAFKA_REQ_BATCH_MSGS*4096, # in Marconi,messages can be up to 4k
            iter_timeout=None,
        )
        self.logger.debug("KafkaDriver: seeking to head of %s" % (self.queue_name))
        self.consumer.seek(0,0) # seek to head of topic; TODO: should get starting position from starting_marker param

        self.periodically_check_for_new_messages() # kick of periodic attainment of new messages (space permitting)

    def periodically_check_for_new_messages(self):
        #self.logger.debug("KafkaDriver.periodically_check_for_new_messages()")
        if self.get_message_stream is not None: # still providing messages
            self.check_for_new_messages()
            # TODO: call call_soon() rather than call_later() if we got some messages and there is still space available in the MessageStream
            self.new_msg_check_callback = self.event_loop.call_later(self.MESSAGE_CHECK_FREQ, self.periodically_check_for_new_messages) # schedules self to run again after MESSAGE_CHECK_FREQ seconds

    def check_for_new_messages(self):
        self.logger.debug("KafkaDriver.check_for_new_messages (start): space_used=%d, amount_of_space_avail=%d" % (self.get_message_stream.space_used(), self.get_message_stream.amount_of_space_avail()))
        max_number_of_messages = self.get_message_stream.amount_of_space_avail()
        if max_number_of_messages == 0:
            return # no space left to add message, so don't look for any

        # now try to get up to max_number_of_messages messages from the topic, but in a non-blocking manner
        messages = self.consumer.get_messages(count=max_number_of_messages, block=False)
        self.logger.debug("got %d messages from Kafka" % (len(messages)))
        assert len(messages) <= max_number_of_messages

        #add the messages to message stream
        for message_and_offset in messages:
            self.get_message_count += 1
            offset_str = "%016x" % (message_and_offset.offset) # make offset into 16 hex chars
            # construct a new message and add it to stream
            self.get_message_stream.add_message(
                payload = str(message_and_offset.message.value),
                marker = offset_str, # TODO: this is supposed to a value that we use a as start_marker but this doesn't indicate the partition, so is not unique
                id = offset_str,
                ttl = (2**31)-1, # we don't store the original TTL so (for now at least) just send max signed 32 bit int
                age = 0,
            )

        #self.logger.debug("KafkaDriver.check_for_new_messages (end): space_used=%d, amount_of_space_avail=%d" % (self.get_message_stream.space_used(), self.get_message_stream.amount_of_space_avail()))

    # called to let the driver know that there no more messages are needed for the previously requested stream of messages and that it should free up any associated resources.
    def cancel_get_stream(self):
        self.new_msg_check_callback.cancel() # cancel call to periodically_check_for_new_messages()
        self.consumer.stop()
        self.consumer = None
        self.get_message_stream = None
        self.queue_name = None


if __name__ == '__main__':
    # test KafkaDriver
    logging.basicConfig(format='%(asctime)s %(message)s',level=logging.INFO)

    try:
        import asyncio
    except ImportError:
        ## Trollius >= 0.3 was renamed
        import trollius as asyncio
    loop = asyncio.get_event_loop()

    print "in KafkaDriver main code"
    driver = KafkaDriver("",loop)

    print "* KafkaDriver main code: test append operations *"

    driver.prepare_for_append_stream("queue2")
    driver.append("KafkaDriver main message 1",1234)
    driver.append("KafkaDriver main message 2",12345)
    driver.append("KafkaDriver main message 3",123456)
    driver.cancel_append_stream()

    print "\n* KafkaDriver main code: test get operations *"

    def start_consume_dir():
        global checked_outqueue_count
        driver.init_get_stream(outqueue,"queue2","cg1",False,False)
        checked_outqueue_count = 0
        periodic_consume_dir_callback()

    # check for items in outqueue and schedule this routine to run again after 0.5s
    def periodic_consume_dir_callback():
        print "periodic_consume_dir_callback called"
        check_outqueue()
        if checked_outqueue_count <= 10:
            loop.call_later(0.0500, periodic_consume_dir_callback)
        else:
            driver.cancel_get_stream()
            loop.stop()

    # check for work that needs to be done for sending messages (e.g., check if the outqueue has new messages or if messages in the outqueue have been waiting too long)
    # returns true iff we should check for messages again later
    def check_outqueue():
        global checked_outqueue_count
        num_new_messages = outqueue.space_used()
        print "%02d: main found %d new messages in MessageStream" % (checked_outqueue_count, num_new_messages)
        for i in range(0,num_new_messages):
            message = outqueue.remove_first_message()
            if (message.payload is None):
                print "missing payload in MessageStream"
            else:
                print "%s: %s" % (message.marker,message.payload)
        checked_outqueue_count += 1

    outqueue = MessageStream(10)
    loop.call_soon(start_consume_dir)

    try:
        loop.run_forever()
    finally:
        loop.close()

