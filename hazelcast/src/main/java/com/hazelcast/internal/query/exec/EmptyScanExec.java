package com.hazelcast.internal.query.exec;

import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.row.EmptyRowBatch;
import com.hazelcast.sql.impl.row.RowBatch;
import com.hazelcast.internal.query.worker.data.DataWorker;

/**
 * Scan over empty result-set.
 */
public class EmptyScanExec implements Exec {
    /** Singleton instance. */
    public static EmptyScanExec INSTANCE = new EmptyScanExec();

    private EmptyScanExec() {
        // No-op.
    }

    @Override
    public void setup(QueryContext ctx, DataWorker worker) {
        // No-op.
    }

    @Override
    public IterationResult advance() {
        return IterationResult.FETCHED_DONE;
    }

    @Override
    public RowBatch currentBatch() {
        return EmptyRowBatch.INSTANCE;
    }
}
