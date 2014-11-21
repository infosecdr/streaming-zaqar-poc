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

import MarconiStreamingAPI_pb2
from importlib import import_module

# see ProtoBufStream section in "Design notes for PoC implementation of streaming API server for Queuing Streaming Interface"

class ProtoBufStream:

    def __init__(self):
        self.start_offset = 0
        self.stream_bytes = bytearray()
        self.mv_offset = 0
        self.incomplete_pb = 0
        self.chunk_list = list()

    def append(self, new_data):
        if ( hasattr(self, 'mv') ):
            del self.mv
        if ( self.start_offset != 0 ):
            self.stream_bytes = self.stream_bytes[self.start_offset:]
            self.start_offset = 0
        self.stream_bytes.extend(new_data)
        self.mv = memoryview(self.stream_bytes)

    #print "encoded SetupRequest_append: " + ':'.join(str(x).encode('hex') for x in self.mv[0:])

    def append_and_parse(self, new_data):
        if ( self.incomplete_pb == 0 ):
            length = self.decode_length_prefix(new_data)
            chunk_length = length + self.mv_offset
            if ( chunk_length > len(new_data) ):
                #print "wait for more bytes"
                self.incomplete_pb = 1
                self.stream_bytes = new_data
                return
                #chunk = bytearray()
            #chunk.extend(stream_bytes[self.mv_offset:chunk_length])
            chunk = new_data[self.mv_offset:chunk_length]
            self.chunk_list.append(chunk)
            #print "id of chunk: " + hex(id(chunk))
            #print "encoded SetupRequest_append_and_parse: " + ':'.join(x.encode('hex') for x in chunk)
            if ( chunk_length < len(new_data) ):
                self.append_and_parse(new_data[chunk_length:])
        else:
            self.stream_bytes += new_data
            self.incomplete_pb = 0
            self.append_and_parse(self.stream_bytes)
        #print "append_and_parse: " + chunk

    def parse_off_first_pb(self, message_name):
        if (len(self.chunk_list) == 0):
            #print "No data to return"
            return None

        encoded_pb = self.chunk_list.pop(0)

        pb_module = import_module("MarconiStreamingAPI_pb2")
        pb_class = getattr(pb_module, message_name)
        sr = pb_class()
        sr.ParseFromString(encoded_pb)
        csr = sr.get
        self.encoded_pb = encoded_pb
        self.print_csr(csr)
        return csr

    def shift_first_pb(self):
        if (self.start_offset == len(self.stream_bytes)):
            #print "No data to return"
            return None
        length = self.decode_length_prefix()
        end_index = self.mv_offset + length
        if ( end_index > len(self.stream_bytes) ):
            #TODO: Should we raise an exception here or just return some special value?
            #print "Incomplete buffer."
            #raise Exception("Incomplete buffer. Trying to parse protocol buffer of length: " + str(length))
            return
        encoded_pb = self.mv[self.mv_offset:end_index]
        #print "encoded SetupRequest_shift: " + ':'.join(str(x).encode('hex') for x in encoded_pb)	
        self.start_offset = end_index
        self.encoded_pb = encoded_pb
        return self.encoded_pb.tobytes()

    def decode_length_prefix(self, stream_bytes=None):
        msb_mask = 0x80
        int_mask = 0x7f
        number_of_bytes = 1
        shift = 0
        length = 0
        error_msg = "Error decoding protocol buffer length"
        if ( stream_bytes == None ):
            stream_bytes = self.mv[self.start_offset:]
        for x in stream_bytes:
            try:
                if (x is int):
                    msb = msb_mask & x
                    value = int_mask & x
                else:
                    msb = msb_mask & ord(x)
                    value = int_mask & ord(x)
                value = value << shift
                length = length | value
                if ((number_of_bytes == 5) and (msb == 0x80)):
                    error_msg = "Protocol buffer length exceeded 5 bytes"
                    raise Exception(error_msg)
                    break;
                if (msb == 0):
                    self.mv_offset = self.start_offset + number_of_bytes
                    return length
                number_of_bytes += 1
                shift += 7
            #TODO: Catch specific exceptions here
            except NameError:
                error_msg = "Error parsing protocol buffer length"
                break
        #TODO: Raise specific exception here
        raise Exception(error_msg)
        return -1

    def encode_length_prefix(self, length):
        encoded_length = bytearray()
        while (length > 0):
            current_byte = length & 0x7f
            length = length >> 7
            if (length > 0):
                current_byte = current_byte | 0x80
            encoded_length.append(current_byte)
        return encoded_length

    def print_csr(self, csr):
        print "##############################################################"
        print "queue_name = " + csr.queue_name
        print "echo_requested = " + str(csr.echo_requested)
        print "include_claimed = " + str(csr.include_claimed)
        print "starting_marker = " + csr.starting_marker
        print "##############################################################"
