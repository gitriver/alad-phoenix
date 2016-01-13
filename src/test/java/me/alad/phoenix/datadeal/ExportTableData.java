package me.alad.phoenix.datadeal;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;


/**
 * DateTime: 2015年3月5日 下午4:30:19
 *
 */
public class ExportTableData {
    private static final Logger LOG = LoggerFactory.getLogger(ExportTableData.class);
    private static Map<String, String> users = Maps.newHashMap();


    public static void main(String[] args) {
        String dir = "D:/tmp/";
        if (args.length > 0) {
            dir = args[0];
        }

        readUserMacId(dir + "userid#mac.txt");

        // 导出表数据
        String[] tableNames =
                { "wifi_area_day_stat", "wifi_area_user", "wifi_city_day_stat", "wifi_city_user",
                 "wifi_portal_log", "wifi_province_day_stat", "wifi_province_user", "wifi_shop_day_stat",
                 "wifi_shop_user" };
        for (String tableName : tableNames) {
            exportTableData(tableName, dir);
        }
    }


    /**
     * 读取用户MAC,ID信息
     * 
     * @param fileName
     */
    private static void readUserMacId(String fileName) {
        try (BufferedReader in = new BufferedReader(new FileReader(fileName));) {
            String line = null;
            int i = 0;
            String[] info = null;
            while ((line = in.readLine()) != null) {
                info = line.split("#");
                users.put(info[1], info[0]);
            }
        }
        catch (Exception e) {
            System.out.println("读取用户信息错误," + e.getMessage());
            e.printStackTrace();
        }

    }


    public static void exportTableData(String tableName, String dir) {
//        List<Map<String, byte[]>> list = HBaseUtil.scanReturList(tableName, null, null, null);
//        List<String> datas = Lists.newArrayList();
//        int i = 0;
//        String line = "";
//        String fileName = dir + tableName + ".txt";
//        File file = new File(fileName);
//        if (file.exists()) {
//            file.delete();
//        }
//
//        for (Map<String, byte[]> map : list) {
//            i++;
//            if (tableName.contains("_log")) {
//                line = parseWifiLog(map);
//            }
//            if (tableName.contains("_stat")) {
//                line = parseDayStatRecord(map);
//            }
//            if (tableName.contains("_user")) {
//                line = parseUserRecord(map);
//            }
//            datas.add(line);
//            if (i % 1000 == 0) {
//                writeFileByLines(fileName, datas, true);
//                datas.clear();
//                System.out.println(tableName + "完成1000条");
//            }
//        }
//        writeFileByLines(fileName, datas, true);
//        System.out.println(tableName + "全部完成，共" + i + "条");
    }


    public static String parseWifiLog(Map<String, byte[]> map) {
        StringBuffer sb = new StringBuffer();
        String rowKey = Bytes.toString(map.get("rowKey"));
        int rowKeyLen = rowKey.length();
        String userMac = rowKey.substring(0, rowKeyLen - 23);
        long time = Long.parseLong(rowKey.substring(rowKeyLen - 23, rowKeyLen - 10));
        // 获取格式为mm:ss:SS的毫秒数
        int msTime = (int) (time % 3600000);
        int day = Integer.parseInt(rowKey.substring(rowKeyLen - 10, rowKeyLen - 2));
        int hour = Integer.parseInt(rowKey.substring(rowKeyLen - 2));
        String shopId = Bytes.toString(map.get("shopId"));
        short busiType = (short) Bytes.toInt(map.get("busiType"));
        String provinceId = Bytes.toString(map.get("provinceId"));
        String cityId = Bytes.toString(map.get("cityId"));
        String areaId = Bytes.toString(map.get("areaId"));
        String districtId = Bytes.toString(map.get("districtId"));
        String businesscirid = Bytes.toString(map.get("businessCirId"));
        String apid = Bytes.toString(map.get("apId"));
        String apgid = Bytes.toString(map.get("apGid"));
        String apmac = Bytes.toString(map.get("apMac")).replace(":", "");
        String userId = getUserId(userMac);
        sb.append(day + "@");
        sb.append(hour + "@");
        sb.append(userMac + "@");
        sb.append(msTime + "@");
        sb.append(busiType + "@");
        sb.append(shopId + "@");
        sb.append(provinceId + "@");
        sb.append(cityId + "@");
        sb.append(areaId + "@");
        sb.append(districtId + "@");
        sb.append(businesscirid + "@");
        sb.append(userId + "@");
        sb.append(apid + "@");
        sb.append(apgid + "@");
        sb.append(apmac);
        return sb.toString();
    }


    public static String parseDayStatRecord(Map<String, byte[]> map) {
        StringBuffer sb = new StringBuffer();
        String rowKey = Bytes.toString(map.get("rowKey"));
        int rowKeyLen = rowKey.length();
        String id = rowKey.substring(0, rowKeyLen - 9);
        short busiType = Short.parseShort(rowKey.substring(rowKeyLen - 9, rowKeyLen - 8));
        String day = rowKey.substring(rowKeyLen - 8);
        int dpv = 0;
        int dau = 0;
        int dnu = 0;
        if (map.containsKey("dpv"))
            dpv = Bytes.toInt(map.get("dpv"));
        if (map.containsKey("dau"))
            dau = Bytes.toInt(map.get("dau"));
        if (map.containsKey("dnu"))
            dnu = Bytes.toInt(map.get("dnu"));
        sb.append(id + "@");
        sb.append(day + "@");
        sb.append(busiType + "@");
        sb.append(dpv + "@");
        sb.append(dau + "@");
        sb.append(dnu + "@");
        sb.append(0);
        return sb.toString();
    }


    public static String parseUserRecord(Map<String, byte[]> map) {
        StringBuffer sb = new StringBuffer();
        String rowKey = Bytes.toString(map.get("rowKey"));
        int rowKeyLen = rowKey.length();
        String id = Bytes.toString(map.get("shopId"));
        String userMac = Bytes.toString(map.get("userMac"));
        short busiType = Short.parseShort(rowKey.substring(rowKeyLen - 1));
        String userId = getUserId(userMac);
        sb.append(id + "@");
        sb.append(busiType + "@");
        sb.append(userId + "@");
        sb.append(userMac);
        return sb.toString();
    }


    public static void writeFileByLines(String filePath, List<String> list, boolean append) {
        PrintWriter pw = null;
        try {
            pw =
                    new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filePath,
                        append), "utf-8")), false);
            Iterator<String> it = list.iterator();
            int index = 0;
            while (it.hasNext()) {
                String str = it.next();
                pw.println(str);
                index++;
                if (index % 5000 == 0) {
                    pw.flush();
                }
            }
            pw.flush();
            if (pw != null) {
                pw.close();
            }
        }
        catch (Exception e) {
            LOG.error("写文件异常:" + e.getMessage());
            e.printStackTrace();
        }
    }


    public static String getUserId(String userMac) {
        String mac = convertMac(userMac);
        String userId = "";
        if (users.containsKey(mac)) {
            userId = users.get(mac);
        }
        return userId;
    }


    private static String convertMac(String str) {
        int len = str.length();
        int num = len / 2;
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < num; i++) {
            sb.append(str.substring(i * 2, i * 2 + 2) + ":");
        }
        return sb.substring(0, sb.length() - 1);
    }
}
