package io.seata.rm.datasource.exec.oracle;

import io.seata.common.exception.NotSupportYetException;
import io.seata.common.exception.ShouldNeverHappenException;
import io.seata.common.util.CollectionUtils;
import io.seata.rm.datasource.PreparedStatementProxy;
import io.seata.rm.datasource.StatementProxy;
import io.seata.rm.datasource.exec.StatementCallback;
import io.seata.rm.datasource.exec.constant.SQLTypeConstant;
import io.seata.rm.datasource.exec.handler.AfterHandler;
import io.seata.rm.datasource.exec.handler.AfterHandlerFactory;
import io.seata.rm.datasource.sql.SQLVisitorFactory;
import io.seata.rm.datasource.sql.struct.Field;
import io.seata.rm.datasource.sql.struct.Row;
import io.seata.rm.datasource.sql.struct.TableMeta;
import io.seata.rm.datasource.sql.struct.TableRecords;
import io.seata.sqlparser.SQLInsertRecognizer;
import io.seata.sqlparser.SQLRecognizer;
import io.seata.sqlparser.SQLType;
import org.apache.commons.lang.StringUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author: lyx
 */
public class OracleInsertSelectExecutor extends OracleInsertExecutor {

    private SQLInsertRecognizer insertRecognizer;

    private static final String SELECT = "select";

    private static String selectSQL;

    private AfterHandler afterHandler;


    public OracleInsertSelectExecutor(StatementProxy statementProxy, StatementCallback statementCallback, SQLRecognizer sqlRecognizer) throws SQLException {
        super(statementProxy, statementCallback, sqlRecognizer);
        createInsertRecognizer();
    }

    public void createInsertRecognizer() throws SQLException {
        SQLInsertRecognizer recognizer = (SQLInsertRecognizer) sqlRecognizer;
        // get the sql after insert
        String querySQL = recognizer.getQuerySQL();
        doCreateInsertRecognizer(querySQL);
        afterHandler = prepareAfterHandler();
    }

    private AfterHandler prepareAfterHandler() {
        SQLInsertRecognizer recognizer = (SQLInsertRecognizer) sqlRecognizer;
        return CollectionUtils.isNotEmpty(recognizer.getDuplicateKeyUpdate()) ?
                AfterHandlerFactory.getAfterHandler(SQLTypeConstant.INSERT_ON_DUPLICATE_UPDATE)
                : recognizer.isIgnore() ? AfterHandlerFactory.getAfterHandler(SQLTypeConstant.INSERT_SELECT) : null;
    }

    @Override
    protected TableRecords beforeImage() throws SQLException {
        TableMeta tableMeta = getTableMeta();
        // after image sql the same of before image
        if (io.seata.common.util.StringUtils.isBlank(selectSQL)) {
            selectSQL = buildImageSQL(tableMeta);
        }
        if (CollectionUtils.isEmpty(paramAppenderMap)) {
            return TableRecords.empty(tableMeta);
        }
        return buildTableRecords2(tableMeta, selectSQL, new ArrayList<>(paramAppenderMap.values()));
    }

    @Override
    protected TableRecords afterImage(TableRecords beforeImage) throws SQLException {
        TableMeta tableMeta = getTableMeta();
        if (Objects.nonNull(afterHandler)) {
            String afterSelectSQL = afterHandler.buildAfterSelectSQL(beforeImage);
            return buildTableRecords2(tableMeta, selectSQL + afterSelectSQL, new ArrayList<>(paramAppenderMap.values()));
        }
        if (CollectionUtils.isEmpty(paramAppenderMap)) {
            return super.afterImage(beforeImage);
        }
        throw new NotSupportYetException("can not support the sql with select");
    }


    @Override
    protected Map<SQLType, List<Row>> buildUndoRow(TableRecords beforeImage, TableRecords afterImage) {
        return afterHandler.buildUndoRow(beforeImage, afterImage);
    }

    /**
     * create the real insert recognizer
     *
     * @param querySQL the sql after insert
     * @throws SQLException
     */
    private void doCreateInsertRecognizer(String querySQL) throws SQLException {
        Map<Integer, ArrayList<Object>> parameters = ((PreparedStatementProxy) statementProxy).getParameters();
        List<SQLRecognizer> sqlRecognizers = SQLVisitorFactory.get(querySQL + FOR_UPDATE, getDbType());
        SQLRecognizer selectRecognizer = sqlRecognizers.get(0);
        // use query SQL to get values from databases
        TableRecords tableRecords = buildTableRecords2(getTableMeta(selectRecognizer.getTableName()), querySQL,
                new ArrayList<>(parameters.values()));
        if (CollectionUtils.isNotEmpty(tableRecords.getRows())) {
            StringBuilder valuesSQL = new StringBuilder();
            // build values sql
            valuesSQL.append(" VALUES");
            tableRecords.getRows().forEach(row -> {
                List<Object> values = row.getFields().stream().map(Field::getValue)
                        .map(value -> Objects.isNull(value) ? null : value).collect(Collectors.toList());
                valuesSQL.append("(");
                for (Object value : values) {
                    if (Objects.isNull(value)) {
                        valuesSQL.append((String) null);
                    } else {
                        valuesSQL.append(value);
                    }
                    valuesSQL.append(",");
                }
                valuesSQL.insert(valuesSQL.length() - 1, ")");
            });
            valuesSQL.deleteCharAt(valuesSQL.length() - 1);
            insertRecognizer = (SQLInsertRecognizer) SQLVisitorFactory.get(formatOriginSQL(valuesSQL.toString()), getDbType()).get(0);
        }
    }

    /**
     * from the sql type to get insert params values
     * unless select insert,other in {@link SQLInsertRecognizer#getInsertParamsValue}
     *
     * @return the insert params values
     */
    @Override
    public List<String> getInsertParamsValue() {
        return Objects.nonNull(insertRecognizer) ? insertRecognizer.getInsertParamsValue() : Collections.emptyList();
    }


    /**
     * from the sql type to get insert rows
     * unless select insert,other in {@link SQLInsertRecognizer#getInsertRows(Collection)}
     *
     * @param primaryKeyIndex the primary key index
     * @return the insert rows
     */
    @Override
    public List<List<Object>> getInsertRows(Collection primaryKeyIndex) {
        return Objects.nonNull(insertRecognizer) ? insertRecognizer.getInsertRows(primaryKeyIndex) : Collections.emptyList();
    }

    /**
     * format origin sql
     *
     * @param valueSQL the value after insert sql
     * @return eg: insert into test values(1,1)
     */
    private String formatOriginSQL(String valueSQL) {
        String tableName = this.sqlRecognizer.getTableName();
        String originalSQL = this.sqlRecognizer.getOriginalSQL();
        int index = originalSQL.indexOf(SELECT);
        if (tableName.equalsIgnoreCase(SELECT)) {
            // choose the next select
            index = originalSQL.indexOf(SELECT, index);
        }
        if (index == -1) {
            throw new ShouldNeverHappenException("may be the query sql is not a select SQL");
        }
        return originalSQL.substring(0, index) + valueSQL;
    }
}

