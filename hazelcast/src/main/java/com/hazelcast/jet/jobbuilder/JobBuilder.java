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

package com.hazelcast.jet.jobbuilder;

import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ResourceConfig;
import com.hazelcast.jet.config.ResourceConfigFactory;
import com.hazelcast.jet.config.ResourceType;
import com.hazelcast.jet.pipeline.*;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

// builds jobs
public final class JobBuilder {

    private static final Map<String, Function4<Pipeline, String, JobBuilderInfoMap, ILogger, Object>> sources = new HashMap<>();
    private static final Map<String, Consumer4<Object, String, JobBuilderInfoMap, ILogger>> sinks = new HashMap<>();
    private static final Map<String, Function4<Object, String, JobBuilderInfoMap, ILogger, Object>> transforms = new HashMap<>();

    private static final URL dummyUrl = getDummyURL(); // a valid, dummy URL

    private static @Nonnull URL getDummyURL() {
        try {
            return new URL("file:///dev/null");
        }
        catch (Exception ex) {
            return null; // this will not happen
        }
    }

    private final Object mutex = new Object();
    private final ILogger logger;
    private boolean initialized;
    private Pipeline pipeline;
    private JobConfig jobConfig;
    private final Map<String, Object> stages = new HashMap<>();
    private Pipeline pipelineContext;
    private Object stageContext;

    public JobBuilder(ILogger logger) {

        this.logger = logger;

        synchronized (mutex) {

            if (!initialized) {

                addProvidedStages(new BuiltinStageProvider());

                ServiceLoader<JobBuilderStageProvider> providers = ServiceLoader.load(JobBuilderStageProvider.class);
                for (JobBuilderStageProvider provider : providers) {
                    logger.fine("Registering steps from provider: " + provider);
                    addProvidedStages(provider);
                }

                initialized = true;
            }
        }
    }

    private void addProvidedStages(JobBuilderStageProvider provider)
    {
        SourceStage[] providedSources = provider.getSources();
        if (providedSources != null) {
            for (SourceStage step : providedSources) {
                sources.put(step.getName(), step.getFunction());
            }
        }

        TransformStage[] providedTransforms = provider.getTransforms();
        if (providedTransforms != null) {
            for (TransformStage step : providedTransforms) {
                transforms.put(step.getName(), step.getFunction());
            }
        }

        SinkStage[] providedSinks = provider.getSinks();
        if (providedSinks != null) {
            for (SinkStage step : providedSinks) {
                sinks.put(step.getName(), step.getFunction());
            }
        }
    }

    // gets the job pipeline
    public Pipeline getPipeline() {
        return pipeline;
    }

    // gets the job configuration
    public JobConfig getConfig() {
        return jobConfig;
    }

    // parses a job definition and creates the job pipeline and configuration
    public void parse(JobBuilderInfoMap definition) throws JobBuilderException {

        JobBuilderInfoMap jobDefinition = definition.childAsMap("job");
        enterJobContext(jobDefinition);

        // job/pipeline is a sequence
        JobBuilderInfoList pipelineDefinition = jobDefinition.childAsList("pipeline");

        for (int i = 0; i < pipelineDefinition.size(); i++) {
            JobBuilderInfoMap fragmentDefinition = pipelineDefinition.itemAsMap(i);
            String fragmentName = fragmentDefinition.uniqueChildName();

            // sequence can be either
            // - pipeline: # enter the pipeline context
            // - whatever: # enter the 'whatever' stage context

            if (fragmentName.equals("pipeline")) {
                enterPipelineContext();
            } else {
                enterStageContext(fragmentName);
            }

            JobBuilderInfoList stageDefinitions = fragmentDefinition.childAsList(fragmentName);
            for (int j = 0; j < stageDefinitions.size(); j++)
            {
                JobBuilderInfoMap stageDefinition = stageDefinitions.itemAsMap(j);

                String sourceName = stageDefinition.childAsString("source", false);
                String sinkName = stageDefinition.childAsString("sink", false);
                String transformName = stageDefinition.childAsString("transform", false);

                if (sourceName != null) {
                    addSource(sourceName, stageDefinition);
                    String stageId = stageDefinition.childAsString("stage-id", false);
                    if (stageId != null) {
                        stages.put(stageId, stageContext);
                    }
                }
                else if (sinkName != null) {
                    addSink(sinkName, stageDefinition);
                }
                else if (transformName != null) {
                    addTransform(transformName, stageDefinition);
                    String stageId = stageDefinition.childAsString("stage-id", false);
                    if (stageId != null) {
                        stages.put(stageId, stageContext);
                    }
                }
                else {
                    throw new JobBuilderException("panic: missing source, sink or transform");
                }
            }
        }
    }

    private void enterJobContext(JobBuilderInfoMap jobDefinition) throws JobBuilderException {

        jobConfig = new JobConfig();
        String name = jobDefinition.childAsString("name", false);
        if (name != null) {
            jobConfig.setName(name);
        }

        // TODO: handle more job properties

        // job/resources is a sequence
        JobBuilderInfoList resources = jobDefinition.childAsList("resources", false);
        if (resources != null) {

            for (int i = 0; i < resources.size(); i++) {
                JobBuilderInfoMap n = resources.itemAsMap(i);
                String id = n.childAsString("id");
                ResourceType resourceType = ResourceType.valueOf(n.childAsString("type"));

                // need to add the resource to the config without the actual path,
                // so we're using a dummy URL, and we bypass the path verifications
                ResourceConfig cfg = ResourceConfigFactory.New(dummyUrl, id, resourceType);
                if (jobConfig.getResourceConfigs().putIfAbsent(id, cfg) != null) {
                    throw new IllegalArgumentException("Resource with id:" + id + " already exists.");
                }
            }
        }
    }

    private void enterPipelineContext() {

        if (pipeline == null) {
            pipeline = Pipeline.create();
        }
        pipelineContext = pipeline;
        stageContext = null;
    }

    private void enterStageContext(String stageId) throws JobBuilderException {

        Object stage = stages.get(stageId);
        if (stage == null) {
            throw new JobBuilderException("Unknown stage '" + stageId + "'.");
        }
        pipelineContext = null;
        stageContext = stage;
    }

    private void addSource(String name, JobBuilderInfoMap definition) throws JobBuilderException {

        if (pipelineContext == null || stageContext != null) {
            throw new JobBuilderException("panic: invalid context");
        }
        Function4<Pipeline, String, JobBuilderInfoMap, ILogger, Object> f = sources.get(name);
        if (f == null) {
            throw new JobBuilderException("Unknown source '" + name + "'.");
        }
        stageContext = f.apply(pipelineContext, name, definition, logger);
        pipelineContext = null;
    }

    private void addSink(String name, JobBuilderInfoMap definition) throws JobBuilderException {

        if (pipelineContext != null || stageContext == null) {
            throw new JobBuilderException("panic: invalid context");
        }
        Consumer4<Object, String, JobBuilderInfoMap, ILogger> f = sinks.get(name);
        if (f == null) {
            throw new JobBuilderException("Unknown sink '" + name + "'.");
        }
        f.accept(stageContext, name, definition, logger);
        stageContext = null;
    }

    private void addTransform(String name, JobBuilderInfoMap definition) throws JobBuilderException {
        logger.fine("add transform: " + name);
        if (pipelineContext != null || stageContext == null) {
            throw new JobBuilderException("panic: invalid context");
        }
        Function4<Object, String, JobBuilderInfoMap, ILogger, Object> f = transforms.get(name);
        if (f == null) {
            throw new JobBuilderException("Unknown transform '" + name + "'.");
        }
        stageContext = f.apply(stageContext, name, definition, logger);
    }
}
