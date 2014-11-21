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
import java.io.InputStream;

public class StreamingConsumeResponseHandler implements MessageHandler.Partial<InputStream> {

    @Override
    public void onMessage(InputStream message, boolean isLast) {
        try {
            MarconiStreamingAPI.Status status = MarconiStreamingAPI.Status.newBuilder().mergeFrom(message).build();
            MarconiStreamingAPI.ConsumeResponse consumeResponse = MarconiStreamingAPI.ConsumeResponse.newBuilder().mergeFrom(message).build();
            if(status != null)
                System.out.println(status.toString());
            if(consumeResponse != null)
                System.out.println(consumeResponse.toString());
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
