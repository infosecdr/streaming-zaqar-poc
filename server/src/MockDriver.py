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

import math
import random

class MockDriver:

    def __init__(self, driver_args, event_loop):
        print "driver initialized; driver_args=%s" % (driver_args)
        self.event_loop = event_loop
        self.get_message_stream = None
        # how frequently to add new messages to GET event stream (space permitting), in seconds
        self.MESSAGE_ADD_FREQ = 0.010
        # to make this look more like a realistic driver, don't always return the full max_number_of_messages requested; instead we randomly chose a percent of that between this parameter and 100%
        self.MIN_PART_OF_MAX_NUM_MESSAGES_TO_PROVIDE = 0.75
        # also, allow for some fake failures
        self.FRACTION_MSGS_TO_FAKE_APPEND_ERROR = 0 # 0.15
        # how many message we have created
        self.count = 0

    ######## APPEND direction ########

    # called to tell driver of a new stream of appends than are going to come in; these should go to the end of the named queue
    def prepare_for_append_stream(self, queue_name):
        print "MockDriver prepare_for_append_stream got: queue_name=%s" % (queue_name)

    def append(self, payload, ttl):
        ttl = int(ttl)
        print "MockDriver append got: ttl=%d, payload=%s" % (ttl, payload)
        if random.uniform(0,1) < self.FRACTION_MSGS_TO_FAKE_APPEND_ERROR:
            print "faking error"
            return 400
        return 100

    def cancel_append_stream(self):
        print "MockDriver cancel_append_stream got called"

    ######## GET direction ########

    # called to tell driver that a new stream of messages is needed for return to a client.   message_stream_queue is an instance of MessageStream to use to put messages the driver has available as a response to this request.  Other arguments have same meaning as in the Marconi API.
    def init_get_stream(self, get_message_stream, queue_name, starting_marker, echo_requested, include_claimed):
        print "MockDriver prepare_to_get_messages got: queue_name=%s, echo_requested=%s, include_claimed=%s, starting_marker=%s" % (queue_name,str(echo_requested),str(include_claimed),starting_marker)
        self.get_message_stream = get_message_stream
        self.periodically_check_for_new_messages() # kick of periodic production of new messages (space permitting)

    def periodically_check_for_new_messages(self):
        if self.get_message_stream is not None: # still providing messages
            self.check_for_new_messages()
            self.new_msg_check_callback = self.event_loop.call_later(self.MESSAGE_ADD_FREQ, self.periodically_check_for_new_messages) # schedules self to run again after MESSAGE_ADD_FREQ seconds

    def check_for_new_messages(self):
        print "MockDriver.check_for_new_messages (start): space_used=%d, amount_of_space_avail=%d" % (self.get_message_stream.space_used(), self.get_message_stream.amount_of_space_avail())
        max_number_of_messages = self.get_message_stream.amount_of_space_avail()
        if max_number_of_messages == 0:
            return

        #create a list of MessageAndMetadata up to the max_number_of_messages and add to message stream
        fraction_to_fulfill = random.uniform(self.MIN_PART_OF_MAX_NUM_MESSAGES_TO_PROVIDE,1.0)
        num_messages_to_send = int(math.ceil(fraction_to_fulfill * max_number_of_messages))

        # we always say that we have the max requested number of messages to send
        for i in range(0,num_messages_to_send):
            self.count += 1
            countstr = format(self.count)

            # construct a new message and add it to stream
            self.get_message_stream.add_message(
                payload = "message #"+countstr,
                marker = countstr,
                id = "id-"+countstr,
                ttl = 2000,
                age = 1900,
            )

        print "MockDriver's get_more_messages() added {} messages to stream".format(num_messages_to_send)
        #print "MockDriver.check_for_new_messages (end): space_used=%d, amount_of_space_avail=%d" % (self.get_message_stream.space_used(), self.get_message_stream.amount_of_space_avail())

    # called to let the driver know that there no more messages are needed for the previously requested stream of messages and that it should free up any associated resources.
    def cancel_get_stream(self):
        self.new_msg_check_callback.cancel() # cancel call to periodically_check_for_new_messages()
        self.get_message_stream = None


if __name__ == '__main__':
    # test MockDriver
    print("in MockDriver main code")
    driver = MockDriver("",None)

    driver.prepare_for_append_stream("queue 1")
    driver.append("message 1",1234)
    driver.cancel_append_stream()

    # GET direction needs to be updated for new interface
    # msgs = driver.get_more_messages(110)
    # print len(msgs)
    # print msgs[0]
    # print msgs[len(msgs)-1]
    #
    # msgs = driver.get_more_messages(100)
    # print len(msgs)
    # print msgs[0]
    # print msgs[len(msgs)-1]
