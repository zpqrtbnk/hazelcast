package com.hazelcast.jet.python;

import com.hazelcast.internal.yaml.YamlMapping;
import com.hazelcast.jet.yaml.*;
import com.hazelcast.logging.ILogger;

public class PythonStepProvider implements StepProvider {

    @Override
    public SourceStep[] getSources() {
        return null;
    }

    @Override
    public TransformStep[] getTransforms() {
        return new TransformStep[] {
            // python.streamed
            // python.batch
            new TransformStep("python", PythonStepProvider::transformPython)
        };
    }

    @Override
    public SinkStep[] getSinks() {
        return null;
    }

    // the idea of "batching" stuff is to reduce round trips to the service by grouping requests
    //
    //
    // mapUsingPython: StreamStage<String> -> StreamStage<String>
    //     return s -> s
    //         .mapUsingServiceAsyncBatched(PythonService.factory(cfg), Integer.MAX_VALUE, PythonService::sendRequest)
    //         .setName("mapUsingPython");
    //
    // mapUsingPythonBatch: BatchStage<String> -> BatchStage<String>
    //     return s -> s
    //         .mapUsingServiceAsyncBatched(PythonService.factory(cfg), Integer.MAX_VALUE, PythonService::sendRequest)
    //         .setName("mapUsingPythonBatch");
    //
    // FIXME that one should be obsoleted in favor or REBALANCE? or... these are different things?!
    // mapUsingPythonBatch: (grouping) BatchStage<String> -> BatchStage<String>
    //     return s -> s
    //         .groupingKey(keyFn)
    //         .mapUsingServiceAsyncBatched(PythonService.factory(cfg), Integer.MAX_VALUE, PythonService::sendRequest)
    //         .setName("mapUsingPythonBatch");

    // - transform: python
    //   python-exe: name of the python exe (python, python3...)
    //   preserve-order: true|false
    //   etc...
    private static Object transformPython(Object stageContext, String name, YamlMapping properties, ILogger logger) throws JobBuilderException {

        // not implemented for now
        return stageContext;

        //return s -> s
        //        .mapUsingServiceAsyncBatched(PythonService.factory(cfg), Integer.MAX_VALUE, PythonService::sendRequest)
        //        .setName("mapUsingPython");
    }
}
