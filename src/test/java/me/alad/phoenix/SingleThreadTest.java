package me.alad.phoenix;

import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import me.alad.phoenix.pool.ConnectionManager;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.google.common.collect.Maps;


/**
 * thrift client test
 */
public class SingleThreadTest {
    public static String baseDir = "G:/";
    public static Logger LOG = LoggerFactory.getLogger(SingleThreadTest.class);


    public static void main(String[] args) {
        if (args.length > 0)
            baseDir = args[0];
        LOG.info("程序运行开始");
        Map<String, String> map = Maps.newHashMap();
        // map.put("cls.selfcate",
        // "select count from cls.selfcate where taskid in(6,481) and busidate=20160314");
        // map.put("cls.result",
        // "select * from cls.result where taskid in(6,481) and busidate=20160314");
        // map.put("cls.kwreltext",
        // "select * from cls.kwreltext where taskid in(6,481) and busidate=20160314");
        map.put("cls.textdetail",
            "select * from cls.textdetail where taskid in(6,481) and busidate=20160314");
        // map.put("tag.hottags",
        // "select * from tag.hottags where taskid in(438,480) and busidate=20160314");
        // map.put("tag.texttag",
        // "select * from tag.texttag where taskid in(438,480) and busidate=20160314");
        // map.put("tag.usertag",
        // "select * from tag.usertag where taskid in(438,480) and busidate=20160314");

        final ApplicationContext context =
                new ClassPathXmlApplicationContext("applicationContext-phoenix.xml");
        ConnectionManager connManager = (ConnectionManager) context.getBean("connectionManagerBean");
        for (Map.Entry<String, String> entry : map.entrySet()) {
            String tableName = entry.getKey();
            LOG.info("开始处理{}表", tableName);
            exportData(entry.getKey(), entry.getValue(), connManager);
            LOG.info("表{}处理完成", tableName);
        }
    }


    public static void exportData(String tableName, String sql, ConnectionManager connManager) {
        try {

            PhoenixConnection conn = connManager.getConn();
            List<Map<String, Object>> list = PhoenixUtil.executeQuery(conn, sql);

            PrintWriter out = new PrintWriter(baseDir + tableName + ".txt");

            StringBuffer sb = new StringBuffer();
            char sep = 239;
            int num = 0;
            for (Map<String, Object> row : list) {
                for (Map.Entry<String, Object> entry : row.entrySet()) {
                    if (entry.getValue() != null) {
                        sb.append(entry.getValue());
                    }
                    sb.append(sep);
                }
                out.println(sb.substring(0, sb.length() - 1));
                sb.setLength(0);
                num++;
                if (num > 1000) {
                    LOG.info("表{}已经处理{}条", tableName, num);
                    out.flush();
                    num = 0;
                }
            }
            out.close();
            LOG.info("表{}处理了{}条", tableName, num);
            connManager.closeConn(conn);
        }
        catch (Exception e) {
            LOG.error(e.getMessage());
            e.printStackTrace();
        }
    }
}
