package com.hazelcast.jet.ext.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.task.AbstractInvocationMessageTask;
import com.hazelcast.cluster.Address;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.jet.ext.client.protocol.JetDebugCodec;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.JobPermission;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.impl.operationservice.InvocationBuilder;
import com.hazelcast.spi.impl.operationservice.Operation;

import javax.annotation.Nullable;
import java.security.Permission;
import java.util.function.Function;

public class JetDebugMessageTask extends AbstractInvocationMessageTask<JetDebugCodec.RequestParameters> {

    private final Function<ClientMessage, JetDebugCodec.RequestParameters> decoder;
    private final Function<JetDebugCodec.ResponseParameters, ClientMessage> encoder;

    public JetDebugMessageTask(ClientMessage clientMessage, Node node, Connection connection) {

        this(clientMessage, node, connection, JetDebugCodec::decodeRequest, o -> JetDebugCodec.encodeResponse(o));
    }
    public JetDebugMessageTask(ClientMessage clientMessage, Node node, Connection connection,
                               Function<ClientMessage, JetDebugCodec.RequestParameters> decoder,
                               Function<JetDebugCodec.ResponseParameters, ClientMessage> encoder) {

        super(clientMessage, node, connection);
        this.decoder = decoder;
        this.encoder = encoder;
    }

    @Override
    public Permission getRequiredPermission() {
        String[] actions = actions();
        if (actions != null) {
            return new JobPermission(actions);
        }
        return null;
    }

    @Override
    protected InvocationBuilder getInvocationBuilder(Operation op) {

        Address address = nodeEngine.getMasterAddress();
        if (address == null) {
            throw new RetryableHazelcastException("master not yet known");
        }
        return nodeEngine.getOperationService().createInvocationBuilder(JetServiceBackend.SERVICE_NAME, op, address);
    }

    @Override
    protected Operation prepareOperation() {

        return new JetDebugOperation(parameters.request);
    }

    @Override
    protected JetDebugCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return decoder.apply(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return encoder.apply(createResponse((String) response));
    }

    private static JetDebugCodec.ResponseParameters createResponse(String s) {

        JetDebugCodec.ResponseParameters rp = new JetDebugCodec.ResponseParameters();
        rp.response = s;
        return rp;
    }

    @Override
    public String getServiceName() {
        return JetServiceBackend.SERVICE_NAME;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return "debug";
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }

    @Nullable
    public String[] actions() {
        // will check permission for submitting a job
        // we should probably clear that list, this is a debug task
        return new String[]{ActionConstants.ACTION_SUBMIT};
    }
}
