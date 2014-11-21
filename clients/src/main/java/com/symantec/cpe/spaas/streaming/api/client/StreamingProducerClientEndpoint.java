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

import javax.websocket.*;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;

public class StreamingProducerClientEndpoint extends Endpoint {
    public static RemoteEndpoint.Basic remote;
    private final String queueName;
    public static long maxMessageNumToSend = 0l;
    public static long lastSentMessageNum = 0l;

    public StreamingProducerClientEndpoint(String queueName) {
        this.queueName = queueName;
    }

    @OnOpen
    public void onOpen(Session session, EndpointConfig config) {
        remote = session.getBasicRemote();
        OutputStream outputStream = null;
        try {
            MarconiStreamingAPI.SetupRequest setupRequest = MarconiStreamingAPI.SetupRequest.getDefaultInstance();
            MarconiStreamingAPI.SetupRequest.Builder setupRequestBuilder = setupRequest.newBuilderForType();
            MarconiStreamingAPI.ProduceSetupRequest produceSetupRequest = setupRequest.getAppend();
            MarconiStreamingAPI.ProduceSetupRequest.Builder produceSetupRequestBuilder = produceSetupRequest.newBuilderForType();
            produceSetupRequestBuilder.setQueueName(queueName);
            produceSetupRequest = produceSetupRequestBuilder.build();
            setupRequestBuilder.setAppend(produceSetupRequest);
            setupRequest = setupRequestBuilder.build();
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            setupRequest.writeDelimitedTo(byteArrayOutputStream);
            ByteBuffer byteBuffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
            outputStream = remote.getSendStream();
            outputStream.write(byteBuffer.array());
            outputStream.flush();
            session.addMessageHandler(new StreamingProduceResponseHandler());
        } catch (IOException e) {
            e.printStackTrace();
            try {
                if(outputStream != null)
                    outputStream.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    @OnClose
    public void onClose(Session session, CloseReason reason) {
        System.out.println(reason.getReasonPhrase());
        try {
            WebsocketStreamingProducerClient.client.connectToServer(new StreamingProducerClientEndpoint(queueName), WebsocketStreamingProducerClient.cec, new URI("ws://localhost:9001"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @OnError
    public void onError(Session session, Throwable t) {
        t.printStackTrace();
    }
}
