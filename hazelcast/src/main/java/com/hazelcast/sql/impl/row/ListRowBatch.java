/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.row;

import java.util.List;

/**
 * Batch where rows are organized in a list.
 */
public class ListRowBatch implements RowBatch {
    /** Rows. */
    private final List<Row> rows;

    public ListRowBatch(List<Row> rows) {
        this.rows = rows;
    }

    @Override
    public Row getRow(int idx) {
        return rows.get(idx);
    }

    @Override
    public int getRowCount() {
        return rows.size();
    }
}
