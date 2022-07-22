package io.seata.rm.datasource.exec.handler;

import io.seata.rm.datasource.sql.struct.Row;
import io.seata.rm.datasource.sql.struct.TableRecords;
import io.seata.sqlparser.SQLType;

import java.util.List;
import java.util.Map;

/**
 * @author: lyx
 */
public interface AfterHandler {

    String buildAfterSelectSQL(TableRecords beforeImage);

    Map<SQLType, List<Row>> buildUndoRow(TableRecords beforeImage, TableRecords afterImage);
}
