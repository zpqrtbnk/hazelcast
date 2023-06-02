package com.hazelcast.client.impl.protocol.codec;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.builtin.StringCodec;
import com.hazelcast.internal.yaml.YamlLoader;
import com.hazelcast.internal.yaml.YamlNode;

import static com.hazelcast.client.impl.protocol.ClientMessage.*;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.*;

public final class JetSubmitYamlJobCodec {

    //hex: 0xFE0102
    public static final int REQUEST_MESSAGE_TYPE = 16646402;
    //hex: 0xFE0103
    public static final int RESPONSE_MESSAGE_TYPE = 16646403;
    private static final int REQUEST_JOB_ID_FIELD_OFFSET = PARTITION_ID_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int REQUEST_DRYRUN_FIELD_OFFSET = REQUEST_JOB_ID_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int REQUEST_INITIAL_FRAME_SIZE = REQUEST_DRYRUN_FIELD_OFFSET + BOOLEAN_SIZE_IN_BYTES;
    private static final int RESPONSE_INITIAL_FRAME_SIZE = RESPONSE_BACKUP_ACKS_FIELD_OFFSET + BYTE_SIZE_IN_BYTES;

    private JetSubmitYamlJobCodec() {
    }

    public static class RequestParameters {

        public long jobId;
        public boolean dryRun;
        public YamlNode jobYaml;

        /*
         * True if the lightJobCoordinator is received from the client, false otherwise.
         * If this is false, lightJobCoordinator has the default value for its type.
         */
        //public boolean isLightJobCoordinatorExists;
    }

    public static ClientMessage encodeRequest(long jobId, boolean dryRun, String jobYaml) {

        ClientMessage clientMessage = ClientMessage.createForEncode();
        clientMessage.setContainsSerializedDataInRequest(true);
        clientMessage.setRetryable(false);
        clientMessage.setOperationName("Jet.SubmitYamlJob");
        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[REQUEST_INITIAL_FRAME_SIZE], UNFRAGMENTED_MESSAGE);
        encodeInt(initialFrame.content, TYPE_FIELD_OFFSET, REQUEST_MESSAGE_TYPE);
        encodeInt(initialFrame.content, PARTITION_ID_FIELD_OFFSET, -1);
        encodeLong(initialFrame.content, REQUEST_JOB_ID_FIELD_OFFSET, jobId);
        encodeBoolean(initialFrame.content, REQUEST_DRYRUN_FIELD_OFFSET, dryRun);
        //encodeUUID(initialFrame.content, REQUEST_LIGHT_JOB_COORDINATOR_FIELD_OFFSET, lightJobCoordinator);
        clientMessage.add(initialFrame);
        StringCodec.encode(clientMessage, jobYaml);
        return clientMessage;
    }

    public static RequestParameters decodeRequest(ClientMessage clientMessage) {

        ClientMessage.ForwardFrameIterator iterator = clientMessage.frameIterator();
        RequestParameters request = new RequestParameters();
        ClientMessage.Frame initialFrame = iterator.next();
        request.jobId = decodeLong(initialFrame.content, REQUEST_JOB_ID_FIELD_OFFSET);
        String yamlString = StringCodec.decode(iterator);
        request.jobYaml = YamlLoader.load(yamlString);
//        if (initialFrame.content.length >= REQUEST_LIGHT_JOB_COORDINATOR_FIELD_OFFSET + UUID_SIZE_IN_BYTES) {
//            request.lightJobCoordinator = decodeUUID(initialFrame.content, REQUEST_LIGHT_JOB_COORDINATOR_FIELD_OFFSET);
//            request.isLightJobCoordinatorExists = true;
//        } else {
//            request.isLightJobCoordinatorExists = false;
//        }
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
