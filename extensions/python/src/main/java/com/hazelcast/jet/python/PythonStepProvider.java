package com.hazelcast.jet.python;

import com.hazelcast.jet.jobbuilder.*;
import com.hazelcast.logging.ILogger;

public class PythonStepProvider implements JobBuilderStageProvider {

    @Override
    public SourceStage[] getSources() {
        return null;
    }

    @Override
    public TransformStage[] getTransforms() {
        return new TransformStage[] {
            // python.streamed
            // python.batch
            new TransformStage("python", PythonStepProvider::transformPython)
        };
    }

    @Override
    public SinkStage[] getSinks() {
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
    private static Object transformPython(Object stageContext, String name, JobBuilderInfoMap properties, ILogger logger) throws JobBuilderException {

        // not implemented for now
        return stageContext;

        //return s -> s
        //        .mapUsingServiceAsyncBatched(PythonService.factory(cfg), Integer.MAX_VALUE, PythonService::sendRequest)
        //        .setName("mapUsingPython");
    }
}
