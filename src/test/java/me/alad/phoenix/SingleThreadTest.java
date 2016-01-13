package me.alad.phoenix;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import me.alad.phoenix.pool.ConnectionManager;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.google.common.collect.Lists;


/**
 * thrift client test
 */
public class SingleThreadTest {
    public static void main(String[] args) throws SQLException {
        final ApplicationContext context =
                new ClassPathXmlApplicationContext("applicationContext-client.xml");

        ConnectionManager connManager = (ConnectionManager) context.getBean("connectionManagerBean");
        PhoenixConnection conn = connManager.getConn();
        List<Map<String, Object>> list2 =
                PhoenixUtil.executeQuery(conn, "select * from wifi.portallog  limit 10");
        System.out.println(list2);
        
        String sql =
                "upsert into log.action(createat,sn,country)values(?,next value for logsn,?)";
        List<List> datas = Lists.newArrayList();
        for(int i=0;i<10;i++){
            List row = Lists.newArrayList();
            row.add(i+1);
            row.add("中国");
            datas.add(row);
        }
        PhoenixUtil.updateBatchPreparedStatement(conn, sql, datas);
        
        connManager.closeConn(conn);
    }
}
