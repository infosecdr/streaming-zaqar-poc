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

import javax.websocket.MessageHandler;
import javax.websocket.OnMessage;
import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;

public class StreamingBinaryMessageHandler implements MessageHandler.Partial<ByteBuffer> {

    @OnMessage
    @Override
    public void onMessage(ByteBuffer message, boolean isLast) {
        try {
            byte[] messageBytes = message.array();
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(messageBytes);
            byteArrayInputStream.close();
            MarconiStreamingAPI.ConsumeResponse.Builder consumeResponseBuilder = MarconiStreamingAPI.ConsumeResponse.newBuilder();
            if (consumeResponseBuilder.mergeDelimitedFrom(byteArrayInputStream)) {
                MarconiStreamingAPI.ConsumeResponse consumeResponse = consumeResponseBuilder.build();
                System.out.println(consumeResponse.toString());
                int numberOfMessagesReceived = consumeResponse.getMessagesList().size();
                StreamingConsumerClientEndpoint.numMessagesReceived += numberOfMessagesReceived;
                StreamingConsumerClientEndpoint.receiveQueueSize -= numberOfMessagesReceived;
                if (StreamingConsumerClientEndpoint.receiveQueueSize * 100 / StreamingConsumerClientEndpoint.maxReceiveQueueSize <= 80) {
                    StreamingConsumerClientEndpoint.maxMessageNumToSend = StreamingConsumerClientEndpoint.maxMessageNumToSend + (StreamingConsumerClientEndpoint.maxReceiveQueueSize - StreamingConsumerClientEndpoint.receiveQueueSize);
                    StreamingConsumerClientEndpoint.receiveQueueSize = StreamingConsumerClientEndpoint.maxReceiveQueueSize;
                }
            }

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
