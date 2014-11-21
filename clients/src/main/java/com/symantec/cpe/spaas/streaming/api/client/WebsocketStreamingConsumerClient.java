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


import org.glassfish.tyrus.client.ClientManager;

import javax.websocket.ClientEndpointConfig;
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class WebsocketStreamingConsumerClient {
    private static CountDownLatch messageLatch;
    public static ClientManager client = ClientManager.createClient();
    public static ClientEndpointConfig cec = ClientEndpointConfig.Builder.create().build();
    public static void main(String [] args){
        try {
            String streamingServerHost = args[0];
            String streamingServerPort = args[1];
            String queueName = args[2];
            String marker = args[3];
            messageLatch = new CountDownLatch(1);
            client.connectToServer(new StreamingConsumerClientEndpoint(queueName, marker), cec, new URI("ws://" + streamingServerHost + ":" + streamingServerPort));
            client.getScheduledExecutorService().scheduleAtFixedRate(new SendConsumeReqeuestThread(), 1, 1, TimeUnit.SECONDS);
            messageLatch.await(10000, TimeUnit.DAYS);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}