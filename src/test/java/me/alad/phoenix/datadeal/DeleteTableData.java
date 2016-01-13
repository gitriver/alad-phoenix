package me.alad.phoenix.datadeal;

import me.alad.phoenix.PhoenixUtil;
import me.alad.phoenix.pool.ConnectionManager;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;


/**
 * DateTime: 2015年3月6日 下午3:31:22
 *
 */
public class DeleteTableData {

    private static final Logger LOG = LoggerFactory.getLogger(DeleteTableData.class);


    public static void main(String[] arg) {
        final ApplicationContext context =
                new ClassPathXmlApplicationContext("applicationContext-client.xml");

        ConnectionManager connManager = (ConnectionManager) context.getBean("connectionManagerBean");
        String[] tableNames =
                { "wifi.areadaystat", "wifi.areauser", "wifi.citydaystat", "wifi.cityuser", "wifi.portallog",
                 "wifi.provincedaystat", "wifi.provinceuser", "wifi.shopdaystat", "wifi.shopuser" };
        try {
            for (String tableName : tableNames) {
                deleteTableData(tableName, connManager);
            }
        }
        catch (Exception e) {
            LOG.error("处理失败" + e.getMessage());
            e.printStackTrace();
        }
    }


    public static void deleteTableData(String tableName, ConnectionManager connManager) throws Exception {
        PhoenixConnection conn = connManager.getConn();
        String sql = "delete from " + tableName;
        PhoenixUtil.execute(conn, sql);

        System.out.println("删除表" + tableName + "成功");
    }

}
