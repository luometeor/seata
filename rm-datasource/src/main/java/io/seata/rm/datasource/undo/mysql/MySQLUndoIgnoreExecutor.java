package io.seata.rm.datasource.undo.mysql;

import io.seata.common.exception.ShouldNeverHappenException;
import io.seata.common.util.CollectionUtils;
import io.seata.rm.datasource.sql.struct.Row;
import io.seata.rm.datasource.sql.struct.TableRecords;
import io.seata.rm.datasource.undo.SQLUndoLog;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * @author: lyx
 */
public class MySQLUndoIgnoreExecutor extends MySQLUndoInsertExecutor {

    private TableRecords undoRecords;

    public MySQLUndoIgnoreExecutor(SQLUndoLog sqlUndoLog) {
        super(sqlUndoLog);
        // init
        undoRecords = getUndoRows();
    }

    @Override
    protected String buildUndoSQL() {
        TableRecords undoRows = getUndoRows();
        List<Row> rows = undoRows.getRows();
        if (CollectionUtils.isEmpty(rows)) {
            throw new ShouldNeverHappenException("Invalid UNDO LOG");
        }
        return generateDeleteSql(rows, undoRows);
    }


    @Override
    protected TableRecords getUndoRows() {
        if (Objects.nonNull(undoRecords)) {
            return undoRecords;
        }
        TableRecords afterImage = sqlUndoLog.getAfterImage();
        List<Row> beforeRows = sqlUndoLog.getBeforeImage().getRows();
        Iterator<Row> iterator = afterImage.getRows().iterator();
        // filter exist images from before images
        while (iterator.hasNext()) {
            Row next = iterator.next();
            for (Row row : beforeRows) {
                if (row.equals(next)) {
                    iterator.remove();
                    break;
                }
            }
        }
        return afterImage;
    }
}
