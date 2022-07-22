package io.seata.rm.datasource.exec;

import io.seata.rm.datasource.ConnectionProxy;
import io.seata.rm.datasource.PreparedStatementProxy;
import io.seata.rm.datasource.StatementProxy;
import io.seata.rm.datasource.exec.mysql.MySQLInsertSelectExecutor;
import io.seata.rm.datasource.sql.struct.Field;
import io.seata.rm.datasource.sql.struct.Row;
import io.seata.rm.datasource.sql.struct.TableMeta;
import io.seata.rm.datasource.sql.struct.TableRecords;
import io.seata.sqlparser.SQLInsertRecognizer;
import io.seata.sqlparser.SQLRecognizer;
import io.seata.sqlparser.util.JdbcConstants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author: lyx
 */
class MySQLInsertSelectExecutorTest {

    private static final String ID_COLUMN = "id";
    private static final String USER_ID_COLUMN = "user_id";
    private static final String USER_NAME_COLUMN = "user_name";
    private static final String USER_STATUS_COLUMN = "user_status";
    private static final Integer PK_VALUE = 100;

    private StatementProxy statementProxy;

    private SQLInsertRecognizer sqlInsertRecognizer;

    private TableMeta tableMeta;

    private MySQLInsertSelectExecutorMock insertSelectExecutorMock;

    private final int pkIndex = 0;
    private HashMap<String, Integer> pkIndexMap;

    private String selectSQL = "select *\n" + "from test02\n" + "where id = ?";

    private String tableName = "test01";

    private String originSQL = "insert  ignore into " + tableName + "\n" +
            "select * from test02\n where id = ?;";

    @BeforeEach
    public void init() throws SQLException {
        ConnectionProxy connectionProxy = mock(ConnectionProxy.class);
        when(connectionProxy.getDbType()).thenReturn(JdbcConstants.MYSQL);

        statementProxy = mock(PreparedStatementProxy.class);
        when(statementProxy.getConnectionProxy()).thenReturn(connectionProxy);
        when(statementProxy.getConnection()).thenReturn(connectionProxy);

        StatementCallback statementCallback = mock(StatementCallback.class);
        sqlInsertRecognizer = mock(SQLInsertRecognizer.class);
        when(sqlInsertRecognizer.getQuerySQL()).thenReturn(selectSQL);
        when(sqlInsertRecognizer.getOriginalSQL()).thenReturn(originSQL);
        when(sqlInsertRecognizer.getTableName()).thenReturn(tableName);

        tableMeta = mock(TableMeta.class);
        insertSelectExecutorMock = Mockito.spy(new MySQLInsertSelectExecutorMock(statementProxy, statementCallback, sqlInsertRecognizer));
        doReturn(tableMeta).when(insertSelectExecutorMock).getTableMeta("test02");
        pkIndexMap = new HashMap<String, Integer>() {
            {
                put(ID_COLUMN, pkIndex);
            }
        };

        TableRecords tableRecords = new TableRecords();
        mockRecordsRow(tableRecords);
        doReturn(tableRecords).when(insertSelectExecutorMock).buildTableRecords2(tableMeta, selectSQL, Collections.EMPTY_LIST);
        insertSelectExecutorMock.superCreateInsertRecognizer();
    }


    @Test
    void getInsertRows() {
        String expectResult = "[[1, 2, 3], [4, 5, NULL]]";
        Assertions.assertEquals(expectResult, insertSelectExecutorMock.getInsertRows(pkIndexMap.values()).toString());
    }

    @Test
    void getInsertParamsValue() {
        String expectResult = "[1, 2, 3, 4, 5, NULL]";
        List<String> insertParamsValue = insertSelectExecutorMock.getInsertParamsValue();
        Assertions.assertEquals(expectResult, insertParamsValue.toString());
    }

    @Test
    public void testGetPkValuesByColumn() throws SQLException {
        mockInsertColumns();
        mockInsertRows();
        mockParametersOfOnePk();
        doReturn(tableMeta).when(insertSelectExecutorMock).getTableMeta();
        when(tableMeta.getPrimaryKeyOnlyName()).thenReturn(Arrays.asList(new String[]{ID_COLUMN}));
        List<Object> pkValues = new ArrayList<>();
        pkValues.add(PK_VALUE);
        doReturn(pkIndexMap).when(insertSelectExecutorMock).getPkIndex();
        Map<String, List<Object>> pkValuesList = insertSelectExecutorMock.getPkValuesByColumn();
        Assertions.assertIterableEquals(pkValuesList.get(ID_COLUMN), pkValues);
    }

    private void mockParametersOfOnePk() {
        Map<Integer, ArrayList<Object>> paramters = new HashMap<>(4);
        ArrayList arrayList1 = new ArrayList<>();
        arrayList1.add(PK_VALUE);
        paramters.put(1, arrayList1);
        PreparedStatementProxy psp = (PreparedStatementProxy) this.statementProxy;
        when(psp.getParameters()).thenReturn(paramters);
    }

    private void mockInsertRows() {
        List<List<Object>> rows = new ArrayList<>();
        rows.add(Arrays.asList("?", "?", "?", "?"));
        when(sqlInsertRecognizer.getInsertRows(pkIndexMap.values())).thenReturn(rows);
    }


    private List<String> mockInsertColumns() {
        List<String> columns = new ArrayList<>();
        columns.add(ID_COLUMN);
        columns.add(USER_ID_COLUMN);
        columns.add(USER_NAME_COLUMN);
        columns.add(USER_STATUS_COLUMN);
        when(sqlInsertRecognizer.getInsertColumns()).thenReturn(columns);
        return columns;
    }

    private void mockRecordsRow(TableRecords tableRecords) {
        List<Row> rows = new ArrayList<>();
        Row row01 = new Row();
        Field field01 = new Field();
        field01.setValue(1);
        Field field02 = new Field();
        field02.setValue(2);
        Field field03 = new Field();
        field03.setValue(3);
        List<Field> fields = new ArrayList<>();
        fields.add(field01);
        fields.add(field02);
        fields.add(field03);
        row01.setFields(fields);
        rows.add(row01);

        Row row02 = new Row();
        Field field001 = new Field();
        field001.setValue(4);
        Field field002 = new Field();
        field002.setValue(5);
        Field field003 = new Field();
        field003.setValue(null);
        List<Field> fields01 = new ArrayList<>();
        fields01.add(field001);
        fields01.add(field002);
        fields01.add(field003);
        row02.setFields(fields01);
        rows.add(row02);

        tableRecords.setRows(rows);
    }

    /**
     * the class for mock
     */
    class MySQLInsertSelectExecutorMock extends MySQLInsertSelectExecutor {

        public MySQLInsertSelectExecutorMock(StatementProxy statementProxy, StatementCallback statementCallback, SQLRecognizer sqlRecognizer) throws SQLException {
            super(statementProxy, statementCallback, sqlRecognizer);
        }

        // just for mock
        @Override
        public void createInsertRecognizer() throws SQLException {
        }

        // just for test
        public void superCreateInsertRecognizer() throws SQLException {
            super.createInsertRecognizer();
        }
    }
}