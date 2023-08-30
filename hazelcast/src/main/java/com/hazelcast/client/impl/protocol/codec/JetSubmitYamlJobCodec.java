/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.client.impl.protocol.codec;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.builtin.StringCodec;

import javax.annotation.Nullable;

import java.util.UUID;

import static com.hazelcast.client.impl.protocol.ClientMessage.*;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.*;

public final class JetSubmitYamlJobCodec {

    public static final int REQUEST_MESSAGE_TYPE = 16646402; //hex: 0xFE0102
    public static final int RESPONSE_MESSAGE_TYPE = 16646403; //hex: 0xFE0103
    private static final int REQUEST_JOB_ID_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_DRYRUN_FIELD_OFFSET = REQUEST_JOB_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int REQUEST_LIGHT_JOB_COORDINATOR_FIELD_OFFSET = REQUEST_DRYRUN_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_LIGHT_JOB_COORDINATOR_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + BYTE_SIZE_IN_BYTES;

    private JetSubmitYamlJobCodec() {
    }

    public static class RequestParameters {

        public long jobId;
        public boolean dryRun;
        public String jobDefinition;

        public @Nullable UUID lightJobCoordinator;

        /*
         * True if the lightJobCoordinator is received from the client, false otherwise.
         * If this is false, lightJobCoordinator has the default value for its type.
         */
        public boolean isLightJobCoordinatorExists;
    }

    public static ClientMessage encodeRequest(long jobId, boolean dryRun, String jobYaml, UUID lightJobCoordinator) {

        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setContainsSerializedDataInRequest(true);
        clientMessage.setRetryable(false);
        clientMessage.setOperationName("Jet.SubmitYamlJob");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeInt(initialFrame.content, PARTITION_ID_FIELD_OFFSET, -1);
        encodeLong(initialFrame.content, REQUEST_JOB_ID_FIELD_OFFSET, jobId);
        encodeBoolean(initialFrame.content, REQUEST_DRYRUN_FIELD_OFFSET, dryRun);
        encodeUUID(initialFrame.content, REQUEST_LIGHT_JOB_COORDINATOR_FIELD_OFFSET, lightJobCoordinator);
        clientMessage.add(initialFrame);
        StringCodec.encode(clientMessage, jobYaml);
        return clientMessage;
    }

    public static RequestParameters decodeRequest(ClientMessage clientMessage) {

        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.jobId = decodeLong(initialFrame.content, REQUEST_JOB_ID_FIELD_OFFSET);
        request.dryRun = decodeBoolean(initialFrame.content, REQUEST_DRYRUN_FIELD_OFFSET);
        request.jobDefinition = StringCodec.decode(iterator);
        if (initialFrame.content.length >= REQUEST_LIGHT_JOB_COORDINATOR_FIELD_OFFSET + UUID_SIZE_IN_BYTES) {
            request.lightJobCoordinator = decodeUUID(initialFrame.content, REQUEST_LIGHT_JOB_COORDINATOR_FIELD_OFFSET);
            request.isLightJobCoordinatorExists = true;
        } else {
            request.isLightJobCoordinatorExists = false;
        }
        return request;
    }

    public static ClientMessage encodeResponse() {

        ClientMessage clientMessage = ClientMessage.createForEncode();
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[RESPONSE_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, RESPONSE_MESSAGE_TYPE);
        clientMessage.add(initialFrame);
        return clientMessage;
    }
}
