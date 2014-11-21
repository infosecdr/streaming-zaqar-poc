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
import java.nio.ByteBuffer;

public class StreamingStringMessageHandler implements MessageHandler.Partial<String> {

    @Override
    public void onMessage(String message, boolean isLast) {
        try {
            byte[] messageBytes = message.getBytes();
            MarconiStreamingAPI.Status status = MarconiStreamingAPI.Status.newBuilder().mergeFrom(messageBytes).build();
            MarconiStreamingAPI.ConsumeResponse consumeResponse = MarconiStreamingAPI.ConsumeResponse.newBuilder().mergeFrom(messageBytes).build();
            if(status != null)
                System.out.println(status.toString());
            if(consumeResponse != null)
                System.out.println(consumeResponse.toString());
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
