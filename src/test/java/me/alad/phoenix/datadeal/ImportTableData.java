package me.alad.phoenix.datadeal;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;

import me.alad.phoenix.PhoenixUtil;
import me.alad.phoenix.pool.ConnectionManager;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;


/**
 * DateTime: 2015年3月6日 上午11:47:50
 *
 */
public class ImportTableData {
    private static final Logger LOG = LoggerFactory.getLogger(ImportTableData.class);


    public static void main(String[] args) {
        String dir = "D:/tmp/";
        int batchNum = 100;
        if (args.length > 0) {
            dir = args[0];
        }
        if (args.length > 1) {
            batchNum = Integer.parseInt(args[1]);
        }

        final ApplicationContext context =
                new ClassPathXmlApplicationContext("applicationContext-client.xml");

        ConnectionManager connManager = (ConnectionManager) context.getBean("connectionManagerBean");

        String[] tableNamesOld =
                { "wifi_area_day_stat", "wifi_area_user", "wifi_city_day_stat", "wifi_city_user",
                 "wifi_portal_log", "wifi_province_day_stat", "wifi_province_user", "wifi_shop_day_stat",
                 "wifi_shop_user" };

        String[] tableNames =
                { "wifi.areadaystat", "wifi.areauser", "wifi.citydaystat", "wifi.cityuser", "wifi.portallog",
                 "wifi.provincedaystat", "wifi.provinceuser", "wifi.shopdaystat", "wifi.shopuser" };

        String[] sqls =
                {
                 "upsert into wifi.areadaystat(id,day,busitype,dpv,dau,dnu,dru) values(?,?,?,?,?,?,?)",
                 "upsert into wifi.areauser(id,busitype,userid,usermac) values(?,?,?,?)",
                 "upsert into wifi.citydaystat(id,day,busitype,dpv,dau,dnu,dru) values(?,?,?,?,?,?,?)",
                 "upsert into wifi.cityuser(id,busitype,userid,usermac) values(?,?,?,?)",
                 "upsert into wifi.portallog(day,hour,usermac,time,busitype,shopid,provinceid,cityid,areaid,districtId,businesscirid,userid,apid,apgid,apmac)"
                         + " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                 "upsert into wifi.provincedaystat(id,day,busitype,dpv,dau,dnu,dru) values(?,?,?,?,?,?,?)",
                 "upsert into wifi.provinceuser(id,busitype,userid,usermac) values(?,?,?,?)",
                 "upsert into wifi.shopdaystat(id,day,busitype,dpv,dau,dnu,dru) values(?,?,?,?,?,?,?)",
                 "upsert into wifi.shopuser(id,busitype,userid,usermac) values(?,?,?,?)", };
        int i = 0;
        try {
            for (String tableName : tableNames) {
                importTableData(tableName, tableNamesOld[i], sqls[i], dir, connManager, batchNum);
                i++;
            }
        }
        catch (Exception e) {
            LOG.error("处理失败" + e.getMessage());
            e.printStackTrace();
        }
    }


    public static void importTableData(String tableName, String tableNameOld, String sql, String dir,
            ConnectionManager connManager, int batchNum) throws Exception {

        String fileName = dir + tableNameOld + ".txt";

        BufferedReader in = new BufferedReader(new FileReader(fileName));
        String line = null;
        int i = 0;
        int j = 0;
        List<List> datas = Lists.newArrayList();
        PhoenixConnection conn = connManager.getConn();
        while ((line = in.readLine()) != null) {
            List<Object> listLine = null;
            if (!Strings.isNullOrEmpty(line)) {
                if (tableName.contains("log")) {
                    listLine = parseWifiLogRecord(line);
                }
                if (tableName.contains("stat")) {
                    listLine = parseDayStatRecord(line);
                }
                if (tableName.contains("user")) {
                    listLine = parseUserRecord(line);
                }
                i++;
                if (listLine != null) {
                    j++;
                    datas.add(listLine);
                }
            }
            if (j % batchNum == 0) {
                PhoenixUtil.updateBatchPreparedStatement(conn, sql, datas);
                datas.clear();
                System.out.println(tableName + "完成" + j + "条");
            }
        }
        PhoenixUtil.updateBatchPreparedStatement(conn, sql, datas);
        in.close();
        System.out.println(tableName + "全部完成，共有" + i + "条数据,成功插入" + j + "条数据");
        connManager.closeConn(conn);
    }


    public static List<Object> parseWifiLogRecord(String line) {
        String[] columns = line.split("@");
        if (columns.length == 15) {
            int day = Integer.parseInt(columns[0]);
            short hour = Short.parseShort(columns[1]);
            int msTime = Integer.parseInt(columns[3]);
            short busiType = Short.parseShort(columns[4]);
            return Lists.newArrayList(new Object[] { day, hour, columns[2], msTime, busiType, columns[5],
                                                    columns[6], columns[7], columns[8], columns[9],
                                                    columns[10], columns[11], columns[12], columns[13],
                                                    columns[14] });
        }
        else {
            System.out.println("数据有误" + line);
            return null;
        }
    }


    public static List<Object> parseDayStatRecord(String line) {
        String[] columns = line.split("@");
        if (columns.length == 7) {
            int day = Integer.parseInt(columns[1]);
            short busiType = Short.parseShort(columns[2]);
            int dpv = Integer.parseInt(columns[3]);
            int dau = Integer.parseInt(columns[4]);
            int dnu = Integer.parseInt(columns[5]);
            int dru = Integer.parseInt(columns[6]);
            return Lists.newArrayList(new Object[] { columns[0], day, busiType, dpv, dau, dnu, dru });
        }
        else {
            System.out.println("数据有误" + line);
            return null;
        }
    }


    public static List<Object> parseUserRecord(String line) {
        String[] columns = line.split("@");
        if (columns.length == 4) {
            short busiType = Short.parseShort(columns[1]);
            return Lists.newArrayList(new Object[] { columns[0], busiType, columns[2], columns[3] });
        }
        else {
            System.out.println("数据有误" + line);
            return null;
        }
    }

}
