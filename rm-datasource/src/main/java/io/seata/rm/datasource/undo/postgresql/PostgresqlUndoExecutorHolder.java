/*
 *  Copyright 1999-2019 Seata.io Group.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package io.seata.rm.datasource.undo.postgresql;

import io.seata.common.loader.LoadLevel;
import io.seata.rm.datasource.undo.AbstractUndoExecutor;
import io.seata.rm.datasource.undo.SQLUndoLog;
import io.seata.rm.datasource.undo.UndoExecutorHolder;
import io.seata.sqlparser.util.JdbcConstants;

/**
 * The Type PostgresqlUndoExecutorHolder
 *
 * @author will
 */
@LoadLevel(name = JdbcConstants.POSTGRESQL)
public class PostgresqlUndoExecutorHolder implements UndoExecutorHolder {

    @Override
    public AbstractUndoExecutor getInsertExecutor(SQLUndoLog sqlUndoLog) {
        return new PostgresqlUndoInsertExecutor(sqlUndoLog);
    }

    @Override
    public AbstractUndoExecutor getUpdateExecutor(SQLUndoLog sqlUndoLog) {
        return new PostgresqlUndoUpdateExecutor(sqlUndoLog);
    }

    @Override
    public AbstractUndoExecutor getDeleteExecutor(SQLUndoLog sqlUndoLog) {
        return new PostgresqlUndoDeleteExecutor(sqlUndoLog);
    }

    @Override
    public AbstractUndoExecutor getInsertIgnoreExecutor(SQLUndoLog sqlUndoLog) {
        return null;
    }
}
