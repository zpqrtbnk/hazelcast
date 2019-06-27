package com.hazelcast.internal.query.expression;

import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public class AndPredicate implements Predicate {

    private Predicate left;
    private Predicate right;

    public AndPredicate() {
        // No-op.
    }

    public AndPredicate(Predicate left, Predicate right) {
        this.left = left;
        this.right = right;
    }

    @Override public Boolean eval(QueryContext ctx, Row row) {
        return left.eval(ctx, row) && right.eval(ctx, row);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(left);
        out.writeObject(right);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        left = in.readObject();
        right = in.readObject();
    }
}
