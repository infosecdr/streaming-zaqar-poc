// Copyright 2014 Symantec.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package com.symantec.cpe.spaas.streaming.api.client;

import com.google.protobuf.ByteString;
import org.openstack.marconi.streaming.MarconiStreamingAPI;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class SendProduceRequestThread implements Runnable {
    @Override
    public void run() {
        if (StreamingProducerClientEndpoint.lastSentMessageNum < StreamingProducerClientEndpoint.maxMessageNumToSend) {
            OutputStream outputStream = null;
            MarconiStreamingAPI.ProduceRequest.Builder produceRequestBuilder = MarconiStreamingAPI.ProduceRequest.getDefaultInstance().newBuilderForType();
            produceRequestBuilder.setTtl(1440);
            List<ByteString> payloadsList = new ArrayList<ByteString>();
            for (long i = StreamingProducerClientEndpoint.lastSentMessageNum; i < StreamingProducerClientEndpoint.maxMessageNumToSend; i++) {
                String message = "message #" + i;
                payloadsList.add(ByteString.copyFrom(message.getBytes()));
            }
            produceRequestBuilder.addAllPayloads(payloadsList);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            MarconiStreamingAPI.ProduceRequest produceRequest = produceRequestBuilder.build();
            try {
                produceRequest.writeDelimitedTo(baos);
                ByteBuffer byteBuffer = ByteBuffer.wrap(baos.toByteArray());
                outputStream = StreamingProducerClientEndpoint.remote.getSendStream();
                outputStream.write(byteBuffer.array());
                outputStream.flush();
            } catch (IOException e) {
                e.printStackTrace();
                if (outputStream != null) {
                    try {
                        outputStream.close();
                    } catch (IOException ex) {
                        ex.printStackTrace();
                    }
                }
            }
        }
    }
}
