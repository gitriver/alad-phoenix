package me.alad.phoenix;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import me.alad.phoenix.pool.ConnectionManager;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.google.common.collect.Lists;


/**
 * 单线程进行测试，可以采用如下几种输入：
 * test; 运行test方法
 * sql; 运行输入的单个SQL
 * multi:sql1:sql2:sql3;批量运行多个SQL
 *
 */
public class PhoenixDemo {
    private static final Logger LOG = LoggerFactory.getLogger(PhoenixDemo.class);
    private static PhoenixConnection conn;


    public static void main(String[] args) {
        final ApplicationContext context =
                new ClassPathXmlApplicationContext("applicationContext-client.xml");

        ConnectionManager connManager = (ConnectionManager) context.getBean("connectionManagerBean");

        conn = connManager.getConn();
        cmd();
        // test2();
        connManager.closeConn(conn);
    }


    public static void cmd() {
        BufferedReader bufferReader = new BufferedReader(new InputStreamReader(System.in));

        while (true) {
            System.out.print(">");
            StringBuffer sb = getSQL(bufferReader);
            String sql = sb.toString().trim();
            while (sql.endsWith(";"))
                sql = sql.substring(0, sql.length() - 1);
            if (sql.equals("quit")) {
                System.out.println("bye");
                break;
            }
            else {
                try {
                    long start = System.currentTimeMillis();
                    if ("test".equalsIgnoreCase(sql)) {
                        test();
                    }
                    else if (sql.startsWith("upsert")) {
                        PhoenixUtil.executeUpdate(conn, sql);
                    }
                    else if (sql.startsWith("multi:")) {
                        sql = sql.substring(6);
                        int num = PhoenixUtil.updateBatchStatement(conn, Arrays.asList(sql.split(":")));
                        System.out.println(num);
                    }
                    else {
                        List<Map<String, Object>> list = PhoenixUtil.executeQuery(conn, sql);
                        System.out.println(list);
                    }
                    long end = System.currentTimeMillis();
                    System.out.println("time consumed:" + (end - start));
                }
                catch (SQLException ex) {
                    ex.printStackTrace();
                }
            }
        }
    }


    private static StringBuffer getSQL(BufferedReader bufferReader) {
        StringBuffer sb = new StringBuffer();
        try {
            while (true) {
                String line = bufferReader.readLine();
                sb.append(line);
                if (line.endsWith(";")) {
                    System.out.print(">");
                    break;
                }
            }
        }
        catch (IOException ex) {
            ex.printStackTrace();
        }
        return sb;
    }


    public static void test2() {
        try {
            PreparedStatement stmt = conn.prepareStatement("upsert into test(id,name,addr)values(?,?,?)");
            for (int i = 0; i < 200; i++) {
                stmt.setString(1, "AAAA" + i);
                stmt.setString(2, "BBBB" + i);
                stmt.setString(3, "cccc" + i);
                stmt.execute();
            }
            conn.commit();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String getFormatNumber(int num,int len) {
        NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(len);
        nf.setGroupingUsed(false);
        return nf.format(num);
    }

    public static void test() throws SQLException {
        NumberFormat nf = NumberFormat.getInstance();
        PhoenixUtil.execute(conn,
            "create table if not exists test(id varchar not null primary key,name varchar,addr varchar)");
        long start = System.currentTimeMillis();
        List<List> list = Lists.newArrayList();
        int result = 0;
        for (int i = 1; i < 200000; i++) {
            list.add(Arrays.asList(new Object[] { "id" + getFormatNumber(i,5), "zhansan" + i, "tianhe" + i }));
            if (i % 300000 == 0) {
                result +=
                        PhoenixUtil.updateBatchPreparedStatement(conn,
                            "upsert into test(id,name,addr)values(?,?,?)", list);
                list.clear();
            }
        }

        result +=
                PhoenixUtil.updateBatchPreparedStatement(conn, "upsert into test(id,name,addr)values(?,?,?)",
                    list);
        System.out.println(result + "=====================================================");

        long end = System.currentTimeMillis();

        long start2 = System.currentTimeMillis();
        List<Map<String, Object>> list2 =
                PhoenixUtil.executeQuery(conn, "select id,name from test where id='id1'");
        long end2 = System.currentTimeMillis();
        System.out.println((end - start) + "," + (end2 - start2));
        System.out.println(list2);
    }
}