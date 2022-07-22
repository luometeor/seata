package io.seata.rm.datasource.exec.oracle;

import io.seata.common.exception.ShouldNeverHappenException;
import io.seata.common.util.CollectionUtils;
import io.seata.common.util.StringUtils;
import io.seata.rm.datasource.StatementProxy;
import io.seata.rm.datasource.exec.StatementCallback;
import io.seata.rm.datasource.exec.constant.SQLTypeConstant;
import io.seata.rm.datasource.exec.handler.AfterHandler;
import io.seata.rm.datasource.exec.handler.AfterHandlerFactory;
import io.seata.rm.datasource.sql.struct.Row;
import io.seata.rm.datasource.sql.struct.TableMeta;
import io.seata.rm.datasource.sql.struct.TableRecords;
import io.seata.sqlparser.SQLRecognizer;
import io.seata.sqlparser.SQLType;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author: lyx
 */
public class OracleInsertIgnoreExecutor extends OracleInsertExecutor {

    /**
     * before image sql and after image sql,condition is unique index
     */
    private String selectSQL;

    public HashMap<List<String>, List<Object>> getParamAppenderMap() {
        return paramAppenderMap;
    }

    private AfterHandler afterHandler;

    public OracleInsertIgnoreExecutor(StatementProxy statementProxy, StatementCallback statementCallback, SQLRecognizer sqlRecognizer) throws SQLException {
        super(statementProxy, statementCallback, sqlRecognizer);
        afterHandler = AfterHandlerFactory.getAfterHandler(SQLTypeConstant.INSERT_IGNORE);
    }

    @Override
    public TableRecords beforeImage() throws SQLException {
        TableMeta tmeta = getTableMeta();
        // after image sql the same of before image
        if (StringUtils.isBlank(selectSQL)) {
            selectSQL = buildImageSQL(tmeta);
        }
        if (CollectionUtils.isEmpty(paramAppenderMap)) {
            throw new ShouldNeverHappenException("can not find unique param,may be you should add unique key when use the sqlType of " +
                    "insert ignore");
        }
        return buildTableRecords2(tmeta, selectSQL, new ArrayList<>(paramAppenderMap.values()));
    }

    @Override
    protected TableRecords afterImage(TableRecords beforeImage) throws SQLException {
        String afterSelectSQL = afterHandler.buildAfterSelectSQL(beforeImage);
        TableRecords afterImage = buildTableRecords2(getTableMeta(), selectSQL + afterSelectSQL, new ArrayList<>(paramAppenderMap.values()));
        return afterImage;
    }

    @Override
    protected Map<SQLType, List<Row>> buildUndoRow(TableRecords beforeImage, TableRecords afterImage) {
        return afterHandler.buildUndoRow(beforeImage, afterImage);
    }
}
