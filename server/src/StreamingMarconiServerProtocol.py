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
from importlib import import_module
import signal
import datetime
from MessageStream import MessageStream
import MarconiStreamingAPI_pb2
import sys
from enum import Enum
from collections import deque
from ProtoBufStream import ProtoBufStream

from autobahn.asyncio.websocket import WebSocketServerFactory, \
                                       WebSocketServerProtocol

class ServerMsgState(Enum):
    """state of server within a wsmessage"""
    __order__ = 'NEW GETTING APPENDING CLOSING CLOSED'  # only needed in 2.x
    NEW = 1
    GETTING = 2
    APPENDING = 3
    CLOSING = 9
    CLOSED = 10

"""
Streaming WebSockets server interface for Marconi that responds to client requests to append or get items from a queue.  Interface is documented in "A High-Throughput Low-Latency Queuing Streaming API to Complement OpenStack Marconi REST Interface".  The implementation is based on Autobahn|Python and asyncio.  Design notes are in "Design notes for PoC implementation of streaming API server for Queuing Streaming Interface".
"""
class StreamingMarconiServerProtocol(WebSocketServerProtocol):
    # method which contains some one-time set up
    def __init__(self):
        self.logger = logger
        self.logger.info(str(datetime.datetime.now()) + ": __init__ called (driver_class_name="+driver_class_name+")")
        self.wsmsg_count = 0 # count of messages
        self.msg_state = ServerMsgState.CLOSED
        self.create_driver_instance(driver_class_name,driver_args,loop) # create driver using the name and args from our command line
        self.SEND_WORK_CHECK_FREQ_SECS = 0.01
        self.MAX_OUTQUEUE_SIZE = 100 # max num messages in outqueue
        self.reset_per_wsmsg_state()
        # install signal handler (server being stopped)
        loop.add_signal_handler(signal.SIGINT,self.handle_interrupt_signal)

    def create_driver_instance(self,driver_class_name,driver_args,event_loop):
        driver_module= import_module(driver_class_name) # import specified driver class
        if driver_module is None:
            self.logger.error("could not import module %s" % (driver_class_name))
            exit(1)
        driver_class = getattr(driver_module,driver_class_name)
        if driver_module is None:
            self.logger.error("could not import class %s" % (driver_class_name))
            exit(1)
        self.driver = driver_class(driver_args,event_loop)


    # method to do any set/reset of state that is needed before handing a new inbound message
    def reset_per_wsmsg_state(self):
        ## members used for consuming and receiving
        self.inbound_protobuf_stream = ProtoBufStream()
        ## members for consuming direction
        self.last_sent_msgnum = 0 # msg sent counter (message num)
        self.max_msgnum_to_send = 0 # highest message number allowed to send (most recently received)
        self.outqueue = MessageStream(self.MAX_OUTQUEUE_SIZE) # small buffer of messages to send
        self.send_a_marker = False # has a marker been requested?
        self.prev_message_marker = None # marker for the most recently send message
        self.prev_message_id = None # id of the most recently send message
        ## members for producing direction
        self.last_rcvd_msgnum = 0 # the message number of the most recently received message
        self.last_acked_msgnum = 0 # the last message number we sent a status for
        self.max_messages_to_request = 200 # the max number of messages we want to get at a time (probably based on memory or driver limitations)
        self.driver_append_results = [] # list of results we got appending a message to the driver; this is ordered, where the first element corresponds to message number last_acked_msgnum+1 (messages we have already responded to are not stored here)

    def on_new_pb_message(self, encoded_pbmessage):
        """Handles a encoded pbmessage that has arrived"""
        if (self.msg_state == ServerMsgState.CLOSED):
            print "internal error: got a pbmessage, but we are still in CLOSED state"
            exit(1) # TODO: better response to error
        elif (self.msg_state == ServerMsgState.CLOSING):
            print "internal error: got a pbmessage, but we are still in CLOSING state"
            exit(1) # TODO: better response to error
        elif (self.msg_state == ServerMsgState.NEW):
            self.on_new_pb_message_in_new_state(encoded_pbmessage)
        elif (self.msg_state == ServerMsgState.GETTING):
            self.on_new_pb_message_in_getting_state(encoded_pbmessage)
        elif (self.msg_state == ServerMsgState.APPENDING):
            self.on_new_pb_message_in_appending_state(encoded_pbmessage)
        else:
            print "internal error: in an unexpected state: " + self.msg_state
            exit(1) # TODO: better response to error

    def on_new_pb_message_in_new_state(self, encoded_pbmessage):
        try:
            sr = MarconiStreamingAPI_pb2.SetupRequest()
            sr.ParseFromString(encoded_pbmessage)
            # TODO: check if succ
        except:
            print "Unexpected error in parsing SetupRequest:", sys.exc_info()[0]
            exit(1) # TODO: better response to error (close wsmessage with an error)
            #raise

        self.logger.info("got SetupRequest: " + str(sr))

        # look at SetupRequest and validate it
        is_get = sr.HasField('get')
        is_append = sr.HasField('append')
        if (is_get and is_append):
            print "SetupRequest is invalid, it cannot be both a get and append"
            exit(1) # TODO: better response to error (close wsmessage with an error)

        if (is_get):
            csr = sr.get
            self.on_new_get_consume_setup_request(csr)
        elif (is_append):
            psr = sr.append
            self.on_new_append_produce_setup_request(psr)
        else:
            print "SetupRequest is invalid, no operation requested"
            exit(1) # TODO: better response to error (close wsmessage with an error)

    def on_new_get_consume_setup_request(self, csr):
        # verify that the ConsumerSetupRequest has a queue name
        for field in ['queue_name','echo_requested','include_claimed','starting_marker']:
            if not csr.HasField(field):
                print "ConsumerSetupRequest is invalid, it is missing a: " + field
                exit(1) # TODO: better response to error (close wsmessage with an error)
        # TODO: verify user has access to named queue

        # tell driver to start adding messages to outqueue based on queue name, starting marker, echo requested, and include claimed
        self.driver.init_get_stream(self.outqueue,csr.queue_name,csr.starting_marker,csr.echo_requested,csr.include_claimed)

        self.msg_state= ServerMsgState.GETTING
        self.send_a_100_status_pbmessage()
        self.periodically_check_for_send_work() # start separate work to periodically check for new message in outqueue or for messages that should be sent

    def on_new_append_produce_setup_request(self, psr):
        if not psr.HasField('queue_name'):
            print "ConsumerSetupRequest is invalid, it is missing a queue_name"
            exit(1) # TODO: better response to error (close wsmessage with an error)
        # TODO: verify user has access to named queue
        # tell driver to get ready for append request based on queue name
        self.driver.prepare_for_append_stream(psr.queue_name)
        self.msg_state= ServerMsgState.APPENDING

        # create a ProduceSetupResponse with status message with 100 and with max_msgnum_to_send set to 0+max_messages_to_request
        psr = MarconiStreamingAPI_pb2.ProduceSetupResponse()
        self.fill_status_pbmessage(psr.status,100)
        psr.max_msgnum_to_send = 0 + self.max_messages_to_request # give our initial number of messages we can handle as a message number
        # send the ProduceSetupResponse
        self.send_a_pbmessage(psr)


    def on_new_pb_message_in_getting_state(self, encoded_pbmessage):
        try:
            cr = MarconiStreamingAPI_pb2.ConsumeRequest()
            cr.ParseFromString(encoded_pbmessage)
            # TODO: check if succ
        except:
            print "Unexpected error in parsing ConsumeRequest:", sys.exc_info()[0]
            exit(1) # TODO: better response to error (close wsmessage with an error)
            #raise
        self.logger.debug("got ConsumeRequest: " + str(cr))

        send_marker = cr.send_marker
        if send_marker is not None and send_marker:
            # client wants us to send a marker when we can
            self.send_a_marker = True

        max_msgnum_to_send = cr.max_msgnum_to_send
        if max_msgnum_to_send is None:
            print "ConsumeRequest is invalid, it is missing a max_msgnum_to_send"
            exit(1) # TODO: better response to error (close wsmessage with an error)
        else:
            self.logger.debug("old max_msgnum_to_send=" + str(self.max_msgnum_to_send) + "; new=" + str(max_msgnum_to_send) + "; we last provided #" + str(self.last_sent_msgnum))
            self.max_msgnum_to_send = max_msgnum_to_send
            # we may have been allowed more messages, so this a good time to try to send messages
            self.send_messages_now()

    # callback from Autobahn
    def on_new_pb_message_in_appending_state(self, encoded_pbmessage):
        # parse encoded pbmessage as a ProduceRequest as produce-request
        try:
            pr = MarconiStreamingAPI_pb2.ProduceRequest()
            pr.ParseFromString(encoded_pbmessage)
            # TODO: check if succ
        except:
            print "Unexpected error in parsing ProduceRequest:", sys.exc_info()[0]
            exit(1) # TODO: better response to error (close wsmessage with an error)
            #raise
        #self.logger.debug("got ProduceRequest: " + str(pr))

        # validate produce request
        for field in ['ttl']:
            if not pr.HasField(field):
                print "ProduceRequest is invalid, it is missing a \"" + field + "\" field"
                exit(1) # TODO: better response to error (close wsmessage with an error; since we couldn't parse, we don't know what message number the client is on any more)
        if len(pr.payloads) == 0:
            print "ProduceRequest is invalid, the payloads field is empty"
            exit(1) # TODO: better response to error (might be able to ignore)
        elif len(pr.payloads) > self.max_messages_to_request:
            print "ProduceRequest is invalid, the there are more messages that we said we could handle (we always say %d max, but this ProduceRequest contained %d" % (self.max_messages_to_request, len(pr.payloads))
            exit(1) # TODO: better response to error (close wsmessage with an error)

        # mark that we have received more messages
        self.last_rcvd_msgnum += len(pr.payloads)
        self.logger.debug("got %d payloads to append, so last_rcvd_msgnum is now %d" % (len(pr.payloads), self.last_rcvd_msgnum))

        # give the driver each of the new messages and collect the results
        # we store the result for message number N in self.driver_append_results[N-last_acked_msgnum-1]
        for payload in pr.payloads:
            # possible TODO: switch to some other way of driver representing result back to us (e.g., abstract them from status codes and/or provide a way to get a specific reason phrase)
            status_code = self.driver.append(payload,pr.ttl)
            # append driver's response to driver_append_result
            self.driver_append_results.append(status_code)
        assert (self.last_acked_msgnum + len(self.driver_append_results)) == self.last_rcvd_msgnum

        # now share result of appends
        # possible TODO: in principle this feedback to the client could be asynchronously, but we do want that feedback to be quite timely
        while (self.last_acked_msgnum < self.last_rcvd_msgnum):
            self.logger.debug("last_acked_msgnum=%d, last_rcvd_msgnum=%d, |driver_append_results|=%d" % (self.last_acked_msgnum, self.last_rcvd_msgnum, len(self.driver_append_results)))
            # find the number of entries in driver_append_result that are identical (same status) to the first entry, left shifting off these entries
            status_to_report = self.driver_append_results.pop(0) # let's see how many consecutive entries have the same status to report
            last_msgnum_with_same_status = self.last_acked_msgnum + 1
            while (last_msgnum_with_same_status < self.last_rcvd_msgnum) and (self.driver_append_results[0] == status_to_report):
                self.driver_append_results.pop(0)
                last_msgnum_with_same_status += 1
            # create a ProduceResponse based on this status
            pr= MarconiStreamingAPI_pb2.ProduceResponse()
            self.fill_status_pbmessage(pr.status,status_to_report)
            pr.msgnum_status_is_thru = last_msgnum_with_same_status
            pr.max_msgnum_to_send = self.last_rcvd_msgnum + self.max_messages_to_request
            self.send_a_pbmessage(pr)
            self.last_acked_msgnum = last_msgnum_with_same_status
        self.logger.debug("last_acked_msgnum=%d, last_rcvd_msgnum=%d, |driver_append_results|=%d" % (self.last_acked_msgnum, self.last_rcvd_msgnum, len(self.driver_append_results)))
        assert len(self.driver_append_results) == 0 # verify no leftover responses
        assert (self.last_acked_msgnum + len(self.driver_append_results)) == self.last_rcvd_msgnum

    # wrap up anything going on for current wsmessage
    def wrap_up_current_wsmessage(self):
        self.logger.info("starting wrap_up_current_wsmessage()")
        if (self.msg_state == ServerMsgState.CLOSED):
            return
        elif (self.msg_state == ServerMsgState.CLOSING):
            return

        self.msg_state = ServerMsgState.CLOSING # set closing so we won't send more full messages to client, etc
        if (self.msg_state == ServerMsgState.GETTING):
            if self.send_a_marker:
                # we are still on the hook for sending a marker, so the send the one associated with the previous message
                self.send_a_bare_marker(self.prev_message_marker,self.prev_message_id)
        elif (self.msg_state == ServerMsgState.APPENDING):
            self.driver.cancel_append_stream()
        # all done with responses to the client for this wsmessage
        self.endMessage()
        # reset/clear per-wsmessage for consume-direction state including contents of outqueue
        self.reset_per_wsmsg_state()
        self.msg_state = ServerMsgState.CLOSED
        self.logger.info("done with wrap_up_current_wsmessage()")

    # create a Status pbmessage
    def fill_status_pbmessage(self,status,status_code,reason_phrase=None):
        status.status_code = status_code
        if reason_phrase is not None:
            status.reason_phrase = reason_phrase
        return status

    # create a Status pbmessage
    def create_status_pbmessage(self,status_code,reason_phrase=None):
        status = MarconiStreamingAPI_pb2.Status()
        self.fill_status_pbmessage(status,status_code,reason_phrase)
        return status

    def send_a_pbmessage(self,pbmessage):
        try:
            encoded_pbmessage = pbmessage.SerializeToString()
            pbStream = ProtoBufStream()
            len_prefix = pbStream.encode_length_prefix(len(encoded_pbmessage))
            self.sendMessageFrame(len_prefix + encoded_pbmessage)
            # TODO: check if succ
        except:
            print "Unexpected error in sending response:", sys.exc_info()[0]
            exit(1)  # TODO: better response to error

    def send_status_pbmessage(self,status_code,reason_phrase=None):
        status = self.create_status_pbmessage(status_code,reason_phrase)
        self.send_a_pbmessage(status)

    def send_a_100_status_pbmessage(self):
        self.send_status_pbmessage(100)

    # send a MessageAndMetadata to client containing just the given message marker and message ID
    def send_a_bare_marker(self,msg_marker,msg_id):
        msg = MarconiStreamingAPI_pb2.MessageAndMetadata()
        msg.marker = msg_marker
        msg.id = msg_id
        self.send_a_pbmessage(msg)
        # clear send_a_marker since have sent one
        self.send_a_marker = False

    # send messages in outqueue (subject to buffering)
    def buffered_send_messages(self):
        # TODO: implement buffering logic
        # return if outqueue is empty
        # return if last_sent_msgnum >= max_msgnum_to_send
        # do_send = false
        # do_send ||= size of outqueue > PREFERED_MIN_NUM_MSGS_TO_SEND
        # do_send ||= (now - enqueue time for first item in outqueue) > MAX_WAIT_TO_SEND
        # if do_send invoke send_messages_now
        self.send_messages_now()

    def send_messages_now(self):
        if self.msg_state != ServerMsgState.GETTING:
            # if we are not in GETTING state we should not be sending new messages
            return

        num_mess_to_send = min(self.max_msgnum_to_send-self.last_sent_msgnum, self.outqueue.space_used())

        if num_mess_to_send == 0:
            return

        cr = MarconiStreamingAPI_pb2.ConsumeResponse()

        # TODO: when we add the buffering logic, we will need the enqueue time for each message in outqueue
        self.logger.debug("server interface send_messages_now(): about to grab %d messages from outqueue to send to client" % (num_mess_to_send))
        for i in range(0,num_mess_to_send):
            msg = self.outqueue.remove_first_message()
            new_msg = cr.messages.add()
            new_msg.CopyFrom(msg)

            # msg is an MessageAndMetadata
            if i == num_mess_to_send: # last item we are going to add
                include_marker = self.send_a_marker
                self.send_a_marker = False
                self.prev_message_marker = msg.marker
                self.prev_message_id = msg.id
            else:
                include_marker = False
            if not include_marker:
                new_msg.ClearField('marker')

        status = cr.status
        status.status_code = 100
        # self.logger.debug("server interface send_messages_now(): about to send %d messages to client" % (num_mess_to_send))
        self.send_a_pbmessage(cr)
        self.last_sent_msgnum += num_mess_to_send


    # don't add a anOpen method as it seems to not allow the frame interface afterwards
    # def onOpen(self):

    # called by autobahn when a new wsmessage has been started by the client
    def onMessageBegin(self, isBinary):
        self.logger.debug("got new wsmessage from client")
        WebSocketServerProtocol.onMessageBegin(self, isBinary)
        # do one-time and pre-wsmesssage setup
        if (self.wsmsg_count is None): self.__init__()
        self.wsmsg_count += 1
        if(self.msg_state != ServerMsgState.GETTING):
            self.msg_state = ServerMsgState.NEW

        # TODO: verify that isBinary

        # set up a message back to the client for our responses
        self.beginMessage(True)

    # called by autobahn when a new wsmessage frame data has arrived as part of the current inbound wsmessage
    def onMessageFrameBegin(self, length):
        WebSocketServerProtocol.onMessageFrameBegin(self, length)

    # called by autobahn when new data has arrived on the current wsmessage
    def onMessageFrameData(self, payload):
        # length = len(payload)
        self.logger.debug("server got: " + ':'.join(x.encode('hex') for x in payload))

        self.inbound_protobuf_stream.append(payload)
        while (True):
            try:
                encoded_pbmessage = self.inbound_protobuf_stream.shift_first_pb()
                if encoded_pbmessage is None:
                    break
            except:
                print "Error parsing protobuf out of data from client in this message; cannot continue with this wsmessage:", sys.exc_info()[0]
                exit(1)  # TODO: better response to error
            self.on_new_pb_message(encoded_pbmessage)

    # called by autobahn at end of a wsmessage frame that is part of the current inbound wsmessage
    def onMessageFrameEnd(self):
        pass

    # called by autobahn when the client terminates a wsmessage
    def onMessageEnd(self):
        self.wrap_up_current_wsmessage()

    # check for work that needs to be done for sending messages (e.g., check if the outqueue has new messages or if messages in the outqueue have been waiting too long) and schedule this routine to run again after SEND_WORK_CHECK_FREQ_SECS
    def periodically_check_for_send_work(self):
        # self.logger.debug(str(datetime.datetime.now()) + ": periodically_check_for_send_work called")
        check_later = self.check_for_send_work()
        if check_later:
            loop.call_later(self.SEND_WORK_CHECK_FREQ_SECS, self.periodically_check_for_send_work)

    # check for work that needs to be done for sending messages (e.g., check if the outqueue has new messages or if messages in the outqueue have been waiting too long)
    # returns true iff we should check for messages again later
    def check_for_send_work(self):
        if (self.msg_state == ServerMsgState.GETTING):
            self.buffered_send_messages()
            return True
        else:
            self.logger.info("disabling calling check_for_send_work since not in GETTING state")
            return False

    def handle_interrupt_signal(self):
        self.logger.info("got interrupt signal")
        self.wrap_up_current_wsmessage()
        loop.stop()

if __name__ == '__main__':
    print("in server main code")

    logger = logging.getLogger('StreamingMarconiServerProtocol')
    logger.setLevel(logging.INFO)
    console_log_handler = logging.StreamHandler(sys.stdout)
    logger.addHandler(console_log_handler)

    # handle command line args
    if (len(sys.argv) < 3):
        print "Usage: %s <server-host> <server-port> <driver-class-name> [<driver-args>]" % (sys.argv[0])
        exit(1)
    server_host = sys.argv[1]
    server_port = sys.argv[2]
    driver_class_name = sys.argv[3]
    if (len(sys.argv) > 3):
        driver_args = sys.argv[4]
    else:
        driver_args = ""


    def exception_handler(loop,context):
        #print "got exception: " + context["message"];
        print "got exception: " + str(context)
        loop.stop()
        exit(1)

    try:
        import asyncio
    except ImportError:
        ## Trollius >= 0.3 was renamed
        import trollius as asyncio

    server_url = "ws://%s:%d" % (server_host, int(server_port))
    factory = WebSocketServerFactory(server_url, debug = False)
    factory.protocol = StreamingMarconiServerProtocol

    loop = asyncio.get_event_loop()
    coro = loop.create_server(factory, server_host, server_port)
    server = loop.run_until_complete(coro)
    loop.set_exception_handler(exception_handler)

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.close()
        loop.stop()
        loop.close()
