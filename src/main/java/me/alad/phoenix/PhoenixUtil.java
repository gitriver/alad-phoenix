package me.alad.phoenix;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;


/**
 * DateTime: 2015年2月12日 下午3:25:07
 *
 */
public class PhoenixUtil {
    private static final Logger LOG = LoggerFactory.getLogger(PhoenixUtil.class);


    public static int executeUpdate(PhoenixConnection conn, String sql, Object... parameters)
            throws SQLException {
        return executeUpdate(conn, sql, Arrays.asList(parameters));
    }


    public static int executeUpdate(PhoenixConnection conn, String sql, List<Object> parameters)
            throws SQLException {
        LOG.debug("执行SQL开始:" + sql);
        PreparedStatement stmt = null;

        int updateCount;
        try {
            stmt = conn.prepareStatement(sql);
            setParameters(stmt, parameters);
            updateCount = stmt.executeUpdate();
            conn.commit();
        }
        finally {
            close(stmt);
        }
        LOG.debug("执行SQL结束:" + sql);
        return updateCount;
    }


    public static boolean execute(PhoenixConnection conn, String sql, Object... parameters)
            throws SQLException {
        return execute(conn, sql, Arrays.asList(parameters));
    }


    public static boolean execute(PhoenixConnection conn, String sql, List<Object> parameters)
            throws SQLException {
        LOG.debug("执行SQL开始:" + sql);
        PreparedStatement stmt = null;
        boolean result;
        try {
            stmt = conn.prepareStatement(sql);
            setParameters(stmt, parameters);
            result = stmt.execute();
            conn.commit();
        }
        finally {
            close(stmt);
        }
        LOG.debug("执行SQL结束:" + sql);
        return result;
    }


    public static List<Map<String, Object>> executeQuery(PhoenixConnection conn, String sql,
            Object... parameters) throws SQLException {
        return executeQuery(conn, sql, Arrays.asList(parameters));
    }


    public static List<Map<String, Object>> executeQuery(PhoenixConnection conn, String sql,
            List<Object> parameters) throws SQLException {
        LOG.debug("执行SQL开始:" + sql);
        List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();

        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            setParameters(stmt, parameters);
            rs = stmt.executeQuery();

            ResultSetMetaData rsMeta = rs.getMetaData();
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<String, Object>();
                for (int i = 0, size = rsMeta.getColumnCount(); i < size; ++i) {
                    String columName = rsMeta.getColumnLabel(i + 1);
                    Object value = rs.getObject(i + 1);
                    row.put(columName, value);
                }

                rows.add(row);
            }
        }
        finally {
            close(rs);
            close(stmt);
        }
        LOG.debug("执行SQL结束:" + sql);
        return rows;
    }


    public static Map<String, Map<String, Object>> executeQueryReturnMap(PhoenixConnection conn, String sql,
            String key, Object... parameters) throws SQLException {
        return executeQueryReturnMap(conn, sql, key, Arrays.asList(parameters));
    }


    public static Map<String, Map<String, Object>> executeQueryReturnMap(PhoenixConnection conn, String sql,
            String key, List<Object> parameters) throws SQLException {
        LOG.debug("执行SQL开始:" + sql);
        Map<String, Map<String, Object>> rows = Maps.newHashMap();

        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            setParameters(stmt, parameters);
            rs = stmt.executeQuery();

            ResultSetMetaData rsMeta = rs.getMetaData();
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<String, Object>();
                for (int i = 0, size = rsMeta.getColumnCount(); i < size; ++i) {
                    String columName = rsMeta.getColumnLabel(i + 1);
                    Object value = rs.getObject(i + 1);
                    row.put(columName, value);
                }

                rows.put(rs.getString(key), row);
            }
        }
        finally {
            close(rs);
            close(stmt);
        }
        LOG.debug("执行SQL结束:" + sql);
        return rows;
    }


    public static Map<Object, Map<String, Object>> executeQueryMap(String key, Connection conn, String sql,
            Object... parameters) throws SQLException {
        return executeQueryMap(key, conn, sql, Arrays.asList(parameters));
    }


    public static Map<Object, Map<String, Object>> executeQueryMap(String key, Connection conn, String sql,
            List<Object> parameters) throws SQLException {
        LOG.debug("执行SQL开始:" + sql);
        Map<Object, Map<String, Object>> rows = new HashMap<Object, Map<String, Object>>();

        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);

            setParameters(stmt, parameters);

            rs = stmt.executeQuery();

            ResultSetMetaData rsMeta = rs.getMetaData();

            Object keyToValue = null;
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<String, Object>();
                for (int i = 0, size = rsMeta.getColumnCount(); i < size; ++i) {
                    String columName = rsMeta.getColumnLabel(i + 1);
                    Object value = rs.getObject(i + 1);
                    if (key.equalsIgnoreCase(columName)) {
                        keyToValue = value;
                    }
                    row.put(columName, value);
                }

                rows.put(keyToValue, row);
            }
        }
        finally {
            close(rs);
            close(stmt);
        }
        LOG.debug("执行SQL结束:" + sql);
        return rows;
    }


    public static Map<Object, Map<Object, Map<String, Object>>> executeQueryMap(String key1, String key2,
            Connection conn, String sql, Object... parameters) throws SQLException {
        return executeQueryMap(key1, key2, conn, sql, Arrays.asList(parameters));
    }


    public static Map<Object, Map<Object, Map<String, Object>>> executeQueryMap(String key1, String key2,
            Connection conn, String sql, List<Object> parameters) throws SQLException {
        LOG.debug("执行SQL开始:" + sql);
        Map<Object, Map<Object, Map<String, Object>>> rows = Maps.newHashMap();

        Map<Object, Map<String, Object>> record = null;

        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);

            setParameters(stmt, parameters);

            rs = stmt.executeQuery();

            ResultSetMetaData rsMeta = rs.getMetaData();

            Object key1ToValue = null;
            Object key2ToValue = null;
            while (rs.next()) {
                Map<String, Object> row = Maps.newHashMap();
                for (int i = 0; i < rsMeta.getColumnCount(); ++i) {
                    String columName = rsMeta.getColumnLabel(i + 1);
                    Object value = rs.getObject(i + 1);
                    if (key1.equalsIgnoreCase(columName)) {
                        key1ToValue = value;
                    }
                    if (key2.equalsIgnoreCase(columName)) {
                        key2ToValue = value;
                    }
                    row.put(columName, value);
                }

                if (rows.containsKey(key1ToValue)) {
                    Map<Object, Map<String, Object>> data2 = rows.get(key1ToValue);
                    data2.put(key2ToValue, row);
                }
                else {
                    record = Maps.newHashMap();
                    record.put(key2ToValue, row);
                    rows.put(key1ToValue, record);
                }
            }
        }
        finally {
            close(rs);
            close(stmt);
        }
        LOG.debug("执行SQL结束:" + sql);

        return rows;
    }


    public static Map<Object, Map<String, Object>> executeQueryMap1(String key1, String key2,
            Connection conn, String sql, Object... parameters) throws SQLException {
        return executeQueryMap1(key1, key2, conn, sql, Arrays.asList(parameters));
    }


    public static Map<Object, Map<String, Object>> executeQueryMap1(String key1, String key2,
            Connection conn, String sql, List<Object> parameters) throws SQLException {
        LOG.debug("执行SQL开始:" + sql);
        Map<Object, Map<String, Object>> rows = Maps.newHashMap();

        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);

            setParameters(stmt, parameters);

            rs = stmt.executeQuery();

            ResultSetMetaData rsMeta = rs.getMetaData();

            Object key1ToValue = null;
            Object key2ToValue = null;
            while (rs.next()) {
                Map<String, Object> row = Maps.newHashMap();
                for (int i = 0; i < rsMeta.getColumnCount(); ++i) {
                    String columName = rsMeta.getColumnLabel(i + 1);
                    Object value = rs.getObject(i + 1);
                    if (key1.equalsIgnoreCase(columName)) {
                        key1ToValue = value;
                    }
                    if (key2.equalsIgnoreCase(columName)) {
                        key2ToValue = value;
                    }
                    row.put(columName, value);
                }
                rows.put(key1ToValue + "" + key2ToValue, row);
            }
        }
        finally {
            close(rs);
            close(stmt);
        }
        LOG.debug("执行SQL结束:" + sql);

        return rows;
    }


    private static void setParameters(PreparedStatement stmt, List<Object> parameters) throws SQLException {
        for (int i = 0, size = parameters.size(); i < size; ++i) {
            Object param = parameters.get(i);
            stmt.setObject(i + 1, param);
        }
    }


    public static void insertToTable(PhoenixConnection conn, String tableName, Map<String, Object> data)
            throws SQLException {
        String sql = makeInsertToTableSql(tableName, data.keySet());
        List<Object> parameters = new ArrayList<Object>(data.values());
        execute(conn, sql, parameters);
    }


    public static String makeInsertToTableSql(String tableName, Collection<String> names) {
        StringBuilder sql = new StringBuilder() //
            .append("upsert into ") //
            .append(tableName) //
            .append("("); //

        int nameCount = 0;
        for (String name : names) {
            if (nameCount > 0) {
                sql.append(",");
            }
            sql.append(name);
            nameCount++;
        }
        sql.append(") values (");
        for (int i = 0; i < nameCount; ++i) {
            if (i != 0) {
                sql.append(",");
            }
            sql.append("?");
        }
        sql.append(")");
        return sql.toString();
    }


    /**
     * 使用普通的Statement进行批量处理
     *
     * @param sqls
     * @return
     * @throws java.sql.SQLException
     */
    public static int updateBatchStatement(PhoenixConnection conn, List<String> sqls) throws SQLException {
        LOG.debug("执行SQL开始:" + sqls);
        Statement stmt = null;
        int num = 0;
        try {
            stmt = conn.createStatement();
            conn.setAutoCommit(false);
            for (int i = 0; i < sqls.size(); i++) {
                stmt.addBatch(sqls.get(i));

                if ((i + 1) % 100 == 0) {
                    num += sum(stmt.executeBatch());
                    stmt.clearBatch();
                    conn.commit();
                }
            }
            num += sum(stmt.executeBatch());
            conn.commit();
            conn.setAutoCommit(true);
        }
        catch (Exception e) {
            e.printStackTrace();
            try {
                conn.rollback();
            }
            finally {
                close(stmt);
            }
        }
        finally {
            close(stmt);
        }
        LOG.debug("执行SQL结束:" + sqls);
        return num;
    }


    /**
     * 使用PreparedStatement进行批量处理
     *
     * @param sqls
     * @return
     * @throws java.sql.SQLException
     */
    public static int updateBatchPreparedStatement(PhoenixConnection conn, String sql, List<List> datas)
            throws SQLException {
        return updateBatchPreparedStatement(conn, sql, datas, 500000);
    }


    /**
     * 使用PreparedStatement进行批量处理
     *
     * @param sqls
     * @return
     * @throws java.sql.SQLException
     */
    public static int updateBatchPreparedStatement(PhoenixConnection conn, String sql, List<List> datas,
            int batchNum) throws SQLException {
        LOG.debug("执行SQL开始:" + sql);
        PreparedStatement stmt = null;
        int num = 0;
        try {
            stmt = conn.prepareStatement(sql);
            for (int i = 0; i < datas.size(); i++) {
                List data = (List) datas.get(i);
                for (int j = 0; j < data.size(); j++) {
                    stmt.setObject(j + 1, data.get(j));
                }
                stmt.addBatch();
                if ((i + 1) % batchNum == 0) {
                    stmt.executeBatch();
                    num += stmt.getUpdateCount();
                    stmt.clearBatch();
                    conn.commit();
                }
            }
            stmt.executeBatch();
            num += stmt.getUpdateCount();
            conn.commit();
        }
        finally {
            close(stmt);
        }
        LOG.debug("执行SQL结束:" + sql);
        return num;
    }


    private static int sum(int[] arr) {
        int num = 0;
        for (int i = 0; i < arr.length; i++) {
            num += arr[i];
        }
        return num;
    }


    /**
     * 释放资源
     * 
     * @param x
     */
    private final static void close(AutoCloseable x) {
        if (x != null) {
            try {
                x.close();
            }
            catch (Exception e) {
                LOG.error("close error", e);
            }
        }
    }
}
