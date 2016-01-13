package me.alad.phoenix;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryServices;


/**
 * DateTime: 2015年2月1日 上午11:14:13
 *
 */
public class phoenixTest {
    /**
     * 此行此例子，需要先创建stock_symbol表，然后导入测试数据 python bin/psql.py -t STOCK_SYMBOL
     * hadoop04,hadoop02,hadoop03 examples/STOCK_SYMBOL.csv
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String args[]) throws Exception {
        PhoenixConnection conn;
        Properties connProps = new Properties();
        connProps.setProperty(QueryServices.MAX_MUTATION_SIZE_ATTRIB,"100000");
//        connProps.setProperty(QueryServices.IMMUTABLE_ROWS_ATTRIB,"10");
        conn = DriverManager.getConnection("jdbc:phoenix:quickstart.cloudera:2181/hbase",connProps).unwrap(PhoenixConnection.class);
//        System.out.println("got connection");
//        test2(conn);
        System.out.println(conn);
        conn.close();
    }


    public static void test2(PhoenixConnection conn) {
        try {
            PreparedStatement stmt = conn.prepareStatement("upsert into test(id,name,addr)values(?,?,?)");
            for (int i = 0; i < 200; i++) {
                stmt.setInt(1,  i);
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


    public static void test(PhoenixConnection conn) throws SQLException {
        ResultSet rst = conn.createStatement().executeQuery("select * from stock_symbol");
        while (rst.next()) {
            System.out.println(rst.getString(1) + " " + rst.getString(2));
        }
        System.out.println("---" + conn.createStatement().executeUpdate("delete from stock_symbol"));
        conn.commit();
        rst = conn.createStatement().executeQuery("select * from stock_symbol");
        while (rst.next()) {
            System.out.println("-xxx--" + rst.getString(1) + " " + rst.getString(2));
        }
        System.out.println("--yy-"
                + conn.createStatement().executeUpdate(
                    "upsert into stock_symbol values('IBM','International Business Machines')"));
        conn.commit();
        rst.close();
    }
}
