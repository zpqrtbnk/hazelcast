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
import com.hazelcast.jet.config.ResourceType;
import com.hazelcast.jet.pipeline.*;
import com.hazelcast.logging.ILogger;

import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

// builds jobs
public final class JobBuilder {

    private static final Map<String, Function4<Pipeline, String, InfoMap, ILogger, Object>> sources = new HashMap<>();
    private static final Map<String, Consumer4<Object, String, InfoMap, ILogger>> sinks = new HashMap<>();
    private static final Map<String, Function4<Object, String, InfoMap, ILogger, Object>> transforms = new HashMap<>();

    private final Object mutex = new Object();
    private final ILogger logger;
    private boolean initialized;
    private Pipeline pipeline;
    private JobConfig jobConfig;
    private Map<String, Object> stages = new HashMap<>();
    private Pipeline pipelineContext;
    private Object stageContext;

    public JobBuilder(ILogger logger) {

        this.logger = logger;

        synchronized (mutex) {

            if (!initialized) {

                addSteps(new BuiltinStepProvider());

                ServiceLoader<StepProvider> providers = ServiceLoader.load(StepProvider.class);
                for (StepProvider provider : providers) {
                    logger.fine("Registering steps from provider: " + provider);
                    addSteps(provider);
                }

                initialized = true;
            }
        }
    }

    private void addSteps(StepProvider provider)
    {
        SourceStep[] sourceSteps = provider.getSources();
        if (sourceSteps != null) {
            for (SourceStep step : sourceSteps) {
                sources.put(step.getName(), step.getFunction());
            }
        }

        TransformStep[] transformSteps = provider.getTransforms();
        if (transformSteps != null) {
            for (TransformStep step : transformSteps) {
                transforms.put(step.getName(), step.getFunction());
            }
        }

        SinkStep[] sinkSteps = provider.getSinks();
        if (sinkSteps != null) {
            for (SinkStep step : sinkSteps) {
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
    public void parse(InfoMap definition) throws JobBuilderException {

        InfoMap jobDefinition = definition.childAsMap("job");
        //YamlMapping jobMapping = ((YamlMapping) root).childAsMapping("job");
        //if (jobMapping == null) throw new JobBuilderException("panic: missing job declaration");
        enterJobContext(jobDefinition);

        // job/pipeline is a sequence
        InfoList pipelineDefinition = jobDefinition.childAsList("pipeline");
        //YamlSequence pipeline = jobMapping.childAsSequence("pipeline");
        //if (pipeline == null) throw new JobBuilderException("panic: missing job/pipeline declaration");

        for (int i = 0; i < pipelineDefinition.size(); i++) {
            InfoMap fragmentDefinition = pipelineDefinition.itemAsMap(i);
            String fragmentName = fragmentDefinition.uniqueChildName();

            // sequence can be either
            // - pipeline: # enter the pipeline context
            // - whatever: # enter the 'whatever' stage context

            if (fragmentName.equals("pipeline")) {
                enterPipelineContext();
            } else {
                enterStageContext(fragmentName);
            }

            InfoList stageDefinitions = fragmentDefinition.childAsList(fragmentName);
            for (int j = 0; j < stageDefinitions.size(); j++)
            {
                InfoMap stageDefinition = stageDefinitions.itemAsMap(j);

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

    private void enterJobContext(InfoMap jobDefinition) throws JobBuilderException {
        logger.fine("enter job context");
        // TODO: job properties
        jobConfig = new JobConfig();
        String name = jobDefinition.childAsString("name", false);
        if (name != null) {
            logger.fine("  name: " + name);
            jobConfig.setName(name);
        }

        // job/resources is a sequence
        InfoList resources = jobDefinition.childAsList("resources", false);
        if (resources != null) {
            for (int i = 0; i < resources.size(); i++) {
                InfoMap n = resources.itemAsMap(i);
                String id = n.childAsString("id");
                ResourceType resourceType = ResourceType.valueOf(n.childAsString("type"));

                logger.fine("  resource: " + resourceType + " " + id);
                //Map<String, ResourceConfig> jobResources = jobConfig.getResourceConfigs();
                URL url = null;
                try { url = new URL("file:///dev/null"); } catch (Exception e) {
                    throw new JobBuilderException("panic: not an url");
                }
                jobConfig.add(url, id, resourceType); // FIXME public ctor
            }
        }
    }

    private void enterPipelineContext() {
        logger.fine("enter pipeline context");
        if (pipeline == null) {
            pipeline = Pipeline.create();
        }
        pipelineContext = pipeline;
        stageContext = null;
    }

    private void enterStageContext(String stageId) throws JobBuilderException {
        logger.fine("enter stage context '" + stageId + "'");
        Object stage = stages.get(stageId);
        if (stage == null) {
            throw new JobBuilderException("panic: unknown stage '" + stageId + "'");
        }
        pipelineContext = null;
        stageContext = stage;
    }

    // Stage i
    //   GeneralStage i
    //     BatchStage i
    //       BatchStageImpl : AbstractStage
    //     StreamStage i
    //       StreamStageImpl : AbstractStage
    //   SinkStage i
    //     SinkStageImpl : AbstractStage
    //   AbstractStage a
    //     ComputeStageImplBase a
    // StreamSourceStage i
    //   StreamSourceStageImpl

    private void addSource(String name, InfoMap definition) throws JobBuilderException {
        logger.fine("add source: " + name);
        if (pipelineContext == null || stageContext != null) {
            throw new JobBuilderException("panic: invalid context");
        }
        Function4<Pipeline, String, InfoMap, ILogger, Object> f = sources.get(name);
        if (f == null) {
            throw new JobBuilderException("panic: unknown source '" + name + "'");
        }
        stageContext = f.apply(pipelineContext, name, definition, logger);
        pipelineContext = null;
    }

    private void addSink(String name, InfoMap definition) throws JobBuilderException {
        logger.fine("add sink: " + name);
        if (pipelineContext != null || stageContext == null) {
            throw new JobBuilderException("panic: invalid context");
        }
        Consumer4<Object, String, InfoMap, ILogger> f = sinks.get(name);
        if (f == null) {
            throw new JobBuilderException("panic: unknown sink '" + name + "'");
        }
        f.accept(stageContext, name, definition, logger);
        stageContext = null;
    }

    private void addTransform(String name, InfoMap definition) throws JobBuilderException {
        logger.fine("add transform: " + name);
        if (pipelineContext != null || stageContext == null) {
            throw new JobBuilderException("panic: invalid context");
        }
        Function4<Object, String, InfoMap, ILogger, Object> f = transforms.get(name);
        if (f == null) {
            throw new JobBuilderException("panic: unknown transform '" + name + "'");
        }
        stageContext = f.apply(stageContext, name, definition, logger);
    }
}
