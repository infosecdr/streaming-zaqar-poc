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

import org.openstack.marconi.streaming.MarconiStreamingAPI;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class SendConsumeReqeuestThread implements Runnable {
    @Override
    public void run() {
        OutputStream outputStream = null;
        MarconiStreamingAPI.ConsumeRequest.Builder consumeRequestBuilder = MarconiStreamingAPI.ConsumeRequest.getDefaultInstance().newBuilderForType();
        consumeRequestBuilder.setMaxMsgnumToSend(StreamingConsumerClientEndpoint.maxMessageNumToSend);
        consumeRequestBuilder.setSendMarker(false);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        MarconiStreamingAPI.ConsumeRequest consumeRequest = consumeRequestBuilder.build();
        try {
            consumeRequest.writeDelimitedTo(baos);
            ByteBuffer byteBuffer = ByteBuffer.wrap(baos.toByteArray());
            outputStream = StreamingConsumerClientEndpoint.remote.getSendStream();
            outputStream.write(byteBuffer.array());
            outputStream.flush();
        } catch (IOException e) {
            e.printStackTrace();
            if(outputStream != null) {
                try {
                    outputStream.close();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
        }
    }
}
