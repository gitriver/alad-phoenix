package me.alad.phoenix.datadeal;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import me.alad.phoenix.PhoenixUtil;
import me.alad.phoenix.pool.ConnectionManager;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * 为表wifi.portallog新加的字段msec更新值
 * @author ding
 *
 */
public class UpdateMsec {
    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        PhoenixConnection conn = null;
        try {
            String sql = "upsert into wifi.portallog values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            ApplicationContext context = new ClassPathXmlApplicationContext("applicationContext-phoenix.xml");
            ConnectionManager connManager = (ConnectionManager) context.getBean("connectionManagerBean");
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
            conn = connManager.getConn();
            List<Map<String, Object>> datas = PhoenixUtil.executeQuery(conn, "select * from wifi.portallog");
            List<List> rows = new ArrayList<List>();
            for (Map<String, Object> map : datas) {
                String day = map.get("day".toUpperCase()).toString();
                String hour = map.get("hour".toUpperCase()).toString();
                int time = Integer.parseInt(map.get("time".toUpperCase()).toString());
                String usermac = map.get("usermac".toUpperCase()).toString();
                long msec = sdf.parse(day + hour).getTime() + time;

                Object busitype = map.get("busitype".toUpperCase());
                Object shopid = map.get("shopid".toUpperCase());
                Object provinceid = map.get("provinceid".toUpperCase());
                Object cityid = map.get("cityid".toUpperCase());
                Object areaid = map.get("areaid".toUpperCase());
                Object districtid = map.get("districtid".toUpperCase());
                Object businesscirid = map.get("businesscirid".toUpperCase());
                Object userid = map.get("userid".toUpperCase());
                Object apid = map.get("apid".toUpperCase());
                Object apgid = map.get("apgid".toUpperCase());
                Object apmacconstraint = map.get("apmacconstraint".toUpperCase());
                Object type = map.get("type".toUpperCase());
                Object stationid = map.get("stationid".toUpperCase());

                List<Object> row = new ArrayList<Object>();
                row.add(map.get("day".toUpperCase()));
                row.add(map.get("hour".toUpperCase()));
                row.add(usermac);
                row.add(map.get("time".toUpperCase()));
                row.add(busitype);
                row.add(shopid);
                row.add(provinceid);
                row.add(cityid);
                row.add(areaid);
                row.add(districtid);
                row.add(businesscirid);
                row.add(userid);
                row.add(apid);
                row.add(apgid);
                row.add(apmacconstraint);
                row.add(type);
                row.add(stationid);
                row.add(msec);
                rows.add(row);
            }
            PhoenixUtil.updateBatchPreparedStatement(conn, sql, rows);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            if (conn != null) {
                try {
                    conn.close();
                }
                catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        long endTime = System.currentTimeMillis();
        System.out.println(endTime - startTime);

    }

}
