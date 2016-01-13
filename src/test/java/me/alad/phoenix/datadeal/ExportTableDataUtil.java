package me.alad.phoenix.datadeal;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import me.alad.phoenix.PhoenixUtil;
import me.alad.phoenix.pool.ConnectionManager;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;


public class ExportTableDataUtil {

    private static Logger LOG = LoggerFactory.getLogger(ExportTableDataUtil.class);


    public static void main(String args[]) throws SQLException {
//        System.out.println(reverseString("a7b85fef-8406-11e3-8a64-5254003b"));
        LOG.info("程序开始运行");
        long start = System.currentTimeMillis();
        ApplicationContext context = new ClassPathXmlApplicationContext("applicationContext-phoenix.xml");
        ConnectionManager connManager = (ConnectionManager) context.getBean("connectionManagerBean");
        PhoenixConnection conn = connManager.getConn();
        getConfig(conn);
        LOG.info("程序运行结束,共耗时{}ms", System.currentTimeMillis() - start);
        connManager.closeConn(conn);
        
        // //8c3fcb12f97be217d41f5c35
    }


    private static void getConfig(PhoenixConnection conn) {
        List<ExportConfig> tasks = new ArrayList<ExportConfig>();
        InputStream is = ExportTableDataUtil.class.getResourceAsStream("/exportConf.txt");
        try (BufferedReader br = new BufferedReader(new InputStreamReader(is));) {
            String info = null;
            while ((info = br.readLine()) != null) {
                if (info.startsWith("##") || "".equals(info)) {
                    continue;
                }
                String[] datas = info.split("@");
                String sql = datas[0];
                String path = datas[1];
                String fieldDelimiter = datas[2];
                boolean isAppend = Boolean.parseBoolean(datas[3]);
                File file = new File(path);
                if(file.exists()){
                    file.delete();
                }
                
                ExportConfig ec = new ExportConfig();
                ec.setSql(sql);
                ec.setPath(path);
                ec.setFieldDelimiter(fieldDelimiter);
                ec.setAppend(isAppend);
                tasks.add(ec);
            }
            queryWriteFile(conn, tasks);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void queryWriteFile(PhoenixConnection conn ,List<ExportConfig> tasks) {
        for (ExportConfig exportConfig : tasks) {
        try (PrintWriter pw = new PrintWriter(new FileWriter(exportConfig.getPath(),exportConfig.isAppend()));) {
                List<Map<String, Object>> results = PhoenixUtil.executeQuery(conn, exportConfig.getSql());
                System.out.println("===="+results.size());
                StringBuffer columns = new StringBuffer(); //列名
                boolean isWColumn = true;
                for (Map<String, Object> map : results) {
                    StringBuffer sb = new StringBuffer();//数据值
                    for (String key : map.keySet()) {
                        if(isWColumn){
                            columns.append(key).append(exportConfig.getFieldDelimiter());
                        }
                        sb.append(map.get(key) == null||"null".equals(map.get(key)) ? "" :map.get(key)).append(exportConfig.getFieldDelimiter());
                    }
                    
                    sb.deleteCharAt(sb.lastIndexOf(exportConfig.getFieldDelimiter()));
                    
                    if(columns.toString().length() > 0){ //写入列名
                        columns.deleteCharAt(columns.lastIndexOf(exportConfig.getFieldDelimiter()));
                        pw.println(columns);
                        columns = new StringBuffer();
                        isWColumn =false;
                    }
                    
                    pw.println(sb.toString());
                    pw.flush();
                }
            }
        catch (Exception e) {
            e.printStackTrace();
        }
        }
    }
    
    
    /**
     * 将字符串倒序输出，使用reverse函数，效率很高
     * 
     * @param str
     * @return
     */
    public static String reverseString(String str) {
        StringBuffer res = new StringBuffer(str);
        res.reverse();
        return res.toString();
    }

}
