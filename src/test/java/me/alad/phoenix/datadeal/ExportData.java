package me.alad.phoenix.datadeal;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import me.alad.phoenix.PhoenixUtil;
import me.alad.phoenix.pool.ConnectionManager;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;


/**
 * 按天、小时统计使用人数,使用次数，新注册用户
 * 
 * @author ding
 *
 */
public class ExportData {
    private static ApplicationContext context = new ClassPathXmlApplicationContext(
        "applicationContext-phoenix.xml");
    private static ConnectionManager connManager = (ConnectionManager) context
        .getBean("connectionManagerBean");


    public static void main(String[] args) {

        if (args.length != 2) {
            System.out.println("输入参数需要为2");
            return;
        }

        int startDay = Integer.parseInt(args[0]);
        int endDay = Integer.parseInt(args[1]);

        Map<String, Map<String, Object>> pvaus = getPvAu(startDay, endDay);
        Map<String, Map<String, Object>> rus = getRu(startDay, endDay);

        File file = new File("F:\\data.txt");
        if (file.exists()) {
            file.delete();
        }

        try (PrintWriter pw =
                new PrintWriter(new OutputStreamWriter(new BufferedOutputStream(new FileOutputStream(file,
                    true), 1024), "utf-8"));) {

            Set<String> keys = pvaus.keySet();
            for (String string : keys) {
                Map<String, Object> pvau = pvaus.get(string);

                Map<String, Object> ru = rus.get(string);

                DayHourStat dhs = new DayHourStat();

                if (pvau != null) {
                    dhs.setDay(Integer.parseInt(pvau.get("day".toUpperCase()).toString()));
                    dhs.setHour(Integer.parseInt(pvau.get("hour".toUpperCase()).toString()));
                    dhs.setPv(Integer.parseInt(pvau.get("pv".toUpperCase()).toString()));
                    dhs.setAu(Integer.parseInt(pvau.get("au".toUpperCase()).toString()));
                }

                if (ru != null) {
                    dhs.setRu(Integer.parseInt(ru.get("ru".toUpperCase()).toString()));
                }

                pw.println(dhs.toString());

                System.out.println(dhs);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }

    }


    public static Map<String, Map<String, Object>> getRu(int startDay, int endDay) {
        PhoenixConnection conn = connManager.getConn();
        String sql =
                "select day,hour,count(*) ru from wifi.portallog where day between  ? and   ? and busitype =2 group by day,hour order by day,hour";
        List<Map<String, Object>> data = Lists.newArrayList();
        try {
            data = PhoenixUtil.executeQuery(conn, sql, startDay, endDay);
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            try {
                conn.close();
            }
            catch (SQLException e) {
                e.printStackTrace();
            }
        }

        Map<String, Map<String, Object>> keyData = Maps.newLinkedHashMap();

        for (Map<String, Object> map : data) {
            keyData.put(map.get("day".toUpperCase()).toString() + map.get("hour".toUpperCase()).toString(),
                map);
        }

        return keyData;
    }


    public static Map<String, Map<String, Object>> getPvAu(int startDay, int endDay) {
        PhoenixConnection conn = connManager.getConn();
        String sql =
                "select day,hour,count(*) pv,count(distinct usermac) au from wifi.portallog where day between  ? and   ? and busitype in(1,3) group by day,hour order by day,hour";

        List<Map<String, Object>> data = Lists.newArrayList();
        try {
            data = PhoenixUtil.executeQuery(conn, sql, startDay, endDay);
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            try {
                conn.close();
            }
            catch (SQLException e) {
                e.printStackTrace();
            }
        }

        Map<String, Map<String, Object>> keyData = Maps.newLinkedHashMap();
        for (Map<String, Object> map : data) {
            keyData.put(map.get("day".toUpperCase()).toString() + map.get("hour".toUpperCase()).toString(),
                map);
        }
        return keyData;
    }

}
