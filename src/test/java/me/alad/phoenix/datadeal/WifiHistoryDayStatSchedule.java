package me.alad.phoenix.datadeal;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

import me.alad.phoenix.PhoenixUtil;
import me.alad.phoenix.pool.ConnectionManager;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.google.common.collect.Lists;


/**
 * wifi天统计.
 */
public class WifiHistoryDayStatSchedule {

    private static transient Logger logger = LoggerFactory.getLogger(WifiHistoryDayStatSchedule.class);

    private static final String TASKNAME = "wifiHistoryDayStatSchedule";

    private static ApplicationContext context = new ClassPathXmlApplicationContext(
        "applicationContext-client.xml");
    private static ConnectionManager connManager = (ConnectionManager) context
        .getBean("connectionManagerBean");

    private int backTime;


    public WifiHistoryDayStatSchedule(int backTime) {
        this.backTime = backTime;
    }

    // 天统计SQL
    private static final String[] STAT_SQLS =
            {
             "upsert into wifi.apdaystat(id,day,busitype,dru) select apmac,day,3,count(distinct usermac) from wifi.portallog where day=%s and busitype =2 group by apmac,day",
             "upsert into wifi.shopdaystat(id,day,busitype,dru) select shopid,day,3,count(distinct usermac) from wifi.portallog where day=%s and busitype =2 group by shopid,day",
             "upsert into wifi.provincedaystat(id,day,busitype,dru) select provinceid,day,3,count(distinct usermac) from wifi.portallog where day=%s and busitype =2 group by provinceid,day",
             "upsert into wifi.citydaystat(id,day,busitype,dru) select cityid,day,3,count(distinct usermac) from wifi.portallog where day=%s and busitype =2 group by cityid,day",
             "upsert into wifi.areadaystat(id,day,busitype,dru) select areaid,day,3,count(distinct usermac) from wifi.portallog where day=%s and busitype =2 group by areaid,day" };


    /**
     * @param taskName
     *            Object
     * @param ownSign
     *            当前环境名称
     * @return
     * @throws Exception
     */
    public boolean execute(String taskName, String ownSign) {

        boolean execStatus = false;
        long start = System.currentTimeMillis();
        logger.info("{}任务执行开始", taskName);

        for (int i = backTime; i > 0; i--) {
            int beforeNDay = getTodayBeforeNDay(i);
            execStat(taskName, connManager, beforeNDay);
            logger.info("统计{}这天数据结束", beforeNDay);
        }

        logger.info("{}任务执行结束,耗时{}", taskName, System.currentTimeMillis() - start);
        return execStatus;
    }


    /**
     * 执行统计.
     *
     * @param taskName
     * @param connManager
     * @param statDay
     *            统计日期
     * @param execDay
     *            执行日期
     * @param execUser
     *            是否执行user统计.
     * @return
     */
    private boolean execStat(String taskName, ConnectionManager connManager, int statDay) {
        PhoenixConnection conn = connManager.getConn();
        try {
            PhoenixUtil.updateBatchStatement(conn, getExecSql(STAT_SQLS, statDay));
        }
        catch (SQLException e) {
            logger.error("执行任务{}{}天的数据统计失败,原因是{}", taskName, statDay, e.getMessage());
            e.printStackTrace();
            return false;
        }
        finally {
            connManager.closeConn(conn);
        }
        return true;
    }


    // 针对SQL语句进行转换
    public List<String> getExecSql(String[] sqls, int day) {

        List<String> list = Lists.newArrayList();
        for (String sql : sqls) {
            list.add(String.format(sql, day));
        }
        return list;
    }


    /**
     * 获取N天前的日期.
     *
     * @param n
     * @return
     */
    private int getTodayBeforeNDay(int n) {
        Calendar cal = new GregorianCalendar();
        cal.add(Calendar.DATE, -n);
        Date d = cal.getTime();
        SimpleDateFormat sp = new SimpleDateFormat("yyyyMMdd");
        String yesterday = sp.format(d);
        return NumberUtils.toInt(yesterday);
    }


    public static void main(String[] args) {
        int backTime = 10;
        if (args.length > 0) {
            backTime = Integer.parseInt(args[0]);
        }

        WifiHistoryDayStatSchedule task = new WifiHistoryDayStatSchedule(backTime);
        task.execute(TASKNAME, "");
    }
}