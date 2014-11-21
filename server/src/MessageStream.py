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

from collections import deque
import MarconiStreamingAPI_pb2

class MessageStream:
    """
    MessageStream is a class representing a finite-length buffer of messages in a stream.  This is used for the driver providing messages to the server interface for get requests.  The following methods are available to the driver:
    """
    # TODO: when we add the buffering logic in interface, we will need the enqueue time for each message

    # buffer_len is the max length of the buffer
    def __init__(self,buffer_len):
        self.buffer= deque([],buffer_len) # storage for the buffer

    # how much space in the buffer is currently used
    def space_used(self):
        return len(self.buffer)

    # is the buffer empty?
    def is_empty(self):
        return len(self.buffer) == 0

    def remove_first_message(self):
        return self.buffer.popleft()

    # this method returns how many messages the buffer can currently accommodate.
    def amount_of_space_avail(self):
        return self.buffer.maxlen - len(self.buffer)

    # this method adds a message to the message stream.  The payload, marker, id (message ID), ttl, and age are the same as in Marconi.  claim_id and claim_client_id have the same meaning as in Marconi and must be provided if relevant.
    def add_message(self,payload,marker,id,ttl,age,claim_id=None,claim_client_id=None):
        # construct a new message
        msg = MarconiStreamingAPI_pb2.MessageAndMetadata()
        msg.payload = payload
        msg.marker = marker
        msg.id = id
        msg.ttl = ttl
        msg.age = age
        if (claim_id is not None):
            msg.claim.id = claim_id
            msg.claim.client_id = claim_client_id

        # add to end of buffer
        self.buffer.append(msg)

if __name__ == '__main__':
    # test MessageStream
    print("in MessageStream main code")

    def show_status(ms,context):
        print "%s: is_empty=%r, space_used=%d, amount_of_space_avail=%d" % (context, ms.is_empty(), ms.space_used(), ms.amount_of_space_avail())

    ms= MessageStream(3)
    show_status(ms,"new")
    ms.add_message("message 1","m1","i1",2000,1)
    show_status(ms,"first message added")
    ms.add_message("message 2","m2","i2",2^31-1,2,"cli2","clci2")
    show_status(ms,"second message added")
    ms.add_message("message 3","m3","i3",2000,3)
    show_status(ms,"third message added")

    msg= ms.remove_first_message()
    show_status(ms,"first message removed")
    print msg

    msg= ms.remove_first_message()
    show_status(ms,"second message removed")
    print msg

    ms.add_message("message 4","m4","i4",2000,0)
    show_status(ms,"fourth message added")

    msg= ms.remove_first_message()
    show_status(ms,"third message removed")
    print msg

    msg= ms.remove_first_message()
    show_status(ms,"fourth message removed")
    print msg


