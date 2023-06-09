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

package com.hazelcast.jet.ext.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.jet.impl.operation.SubmitJobOperation;
import com.hazelcast.jet.ext.yaml.JobBuilder;
import com.hazelcast.jet.ext.yaml.JobBuilderException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.spi.impl.operationservice.Operation;

import javax.annotation.Nullable;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class JetSubmitYamlJobMessageTask extends AbstractJetMessageTask<JetSubmitYamlJobCodec.RequestParameters, Void> {

    private UUID lightJobCoordinator;

    protected JetSubmitYamlJobMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection, JetSubmitYamlJobCodec::decodeRequest, o -> JetSubmitYamlJobCodec.encodeResponse());
    }

    @Override
    protected UUID getLightJobCoordinator() { return lightJobCoordinator; }

    // from AbstractInvocationMessageTask
    @Override
    protected CompletableFuture<Object> processInternal() {

        Operation op;
        try
        {
            op = prepareOperationThrows();
        }
        catch (JobBuilderException e) {
            return CompletableFuture.completedFuture(e);
        }

        if (parameters.dryRun) {
            return CompletableFuture.completedFuture(null);
        }

        op.setCallerUuid(endpoint.getUuid());
        return getInvocationBuilder(op).setResultDeserialized(false).invoke();
    }

    // must be provided but will never get invoked because we override processInternal
    @Override
    protected Operation prepareOperation() { return null; }

    protected Operation prepareOperationThrows() throws JobBuilderException {

        ILogger logger = nodeEngine.getLogger(JobBuilder.class);

        JobBuilder jobBuilder = new JobBuilder(logger);
        jobBuilder.parse(parameters.jobYaml);

        // TODO understand this
        lightJobCoordinator = null;
        boolean isLightJob = false;

        Data jobDefinition = toData(jobBuilder.getPipeline());
        Data jobConfig = toData(jobBuilder.getConfig());

        return new SubmitJobOperation(parameters.jobId,
                null, null,
                jobDefinition, jobConfig,
                isLightJob,
                endpoint.getSubject());
    }

//    @Override
//    protected ClientMessage encodeResponse(Object o) {
//        return super.encodeResponse(o);
//    }
//
//    @Override
//    protected ClientMessage encodeException(Throwable throwable) {
//        return super.encodeException(throwable);
//    }

    @Override
    public String getMethodName() {
        return "submitJobYaml";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{};
    }

    @Nullable
    @Override
    public String[] actions() {
        return new String[]{ActionConstants.ACTION_SUBMIT};
    }
}
