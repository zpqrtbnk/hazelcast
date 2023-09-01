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

package com.hazelcast.jet.jobbuilder.protocol;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.yaml.YamlException;
import com.hazelcast.internal.yaml.YamlLoader;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.operation.SubmitJobOperation;
import com.hazelcast.jet.jobbuilder.JobBuilder;
import com.hazelcast.jet.jobbuilder.JobBuilderException;
import com.hazelcast.jet.jobbuilder.JobBuilderInfoMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.spi.impl.operationservice.Operation;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class JetSubmitYamlJobMessageTask extends AbstractJetMessageTask<JetSubmitYamlJobCodec.RequestParameters, Void> {

    public JetSubmitYamlJobMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection, JetSubmitYamlJobCodec::decodeRequest, o -> JetSubmitYamlJobCodec.encodeResponse());
    }

    @Override
    protected UUID getLightJobCoordinator() {
        return parameters.lightJobCoordinator;
    }

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
    protected Operation prepareOperation() {
        return null;
    }

    protected Operation prepareOperationThrows() throws JobBuilderException {

        ILogger logger = nodeEngine.getLogger(JobBuilder.class);

        Object jobDefinitionObject;
        try {
            jobDefinitionObject = YamlLoader.loadRaw(parameters.jobDefinition);
        }
        catch (YamlException ex) {
            throw new JobBuilderException("An error occurred while loading and parsing the YAML string.", ex.getCause());
        }

        JobBuilderInfoMap jobDefinition;
        try {
            jobDefinition = new JobBuilderInfoMap((Map<String, Object>) jobDefinitionObject);
        }
        catch (Exception ex) {
            throw new JobBuilderException("The YAML string does not expose a map.");
        }

        JobBuilder jobBuilder = new JobBuilder(logger);
        jobBuilder.parse(jobDefinition);

        JobConfig deserializedJobConfig = jobBuilder.getConfig();
        Data serializedJobConfig = null; // no point serializing, will be null

        Object deserializedJobDefinition = jobBuilder.getPipeline();
        Data serializedJobDefinition = null; // the jobDefinition for non-light job *must* be serialized

        boolean isLightJob = parameters.lightJobCoordinator != null;
        if (!isLightJob) {
            serializedJobDefinition = nodeEngine.toData(deserializedJobDefinition);
            deserializedJobDefinition = null;
        }

        return new SubmitJobOperation(parameters.jobId,
                deserializedJobDefinition, deserializedJobConfig,
                serializedJobDefinition, serializedJobConfig,
                isLightJob,
                endpoint.getSubject());
    }

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
