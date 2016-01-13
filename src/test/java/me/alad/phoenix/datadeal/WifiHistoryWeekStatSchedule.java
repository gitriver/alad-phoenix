package me.alad.phoenix.datadeal;

import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

import me.alad.phoenix.PhoenixUtil;
import me.alad.phoenix.pool.ConnectionManager;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.google.common.collect.Lists;


/**
 * wifi统计.
 */
public class WifiHistoryWeekStatSchedule {

    private static transient Logger logger = LoggerFactory.getLogger(WifiHistoryWeekStatSchedule.class);

    private static final String TASKNAME = "wifiHistoryWeekStatSchedule";
    private static ApplicationContext context = new ClassPathXmlApplicationContext(
        "classpath:applicationContext-client.xml");
    private static ConnectionManager connManager = (ConnectionManager) context
        .getBean("connectionManagerBean");

    private int backTime;


    public WifiHistoryWeekStatSchedule(int backTime) {
        this.backTime = backTime;
    }

    private static final String[] AU_SQLS =
            {
             "upsert into wifi.apweekstat(id,year,week,busitype,au) select apmac,%s,%s,busitype,count(distinct usermac) from wifi.portallog where day between %s and %s and busitype in(1,3) group by apmac,busitype",
             "upsert into wifi.shopweekstat(id,year,week,busitype,au) select shopid,%s,%s,busitype,count(distinct usermac) from wifi.portallog where day between %s and %s and busitype in(1,3) group by shopid,busitype",
             "upsert into wifi.provinceweekstat(id,year,week,busitype,au) select provinceid,%s,%s,busitype,count(distinct usermac) from wifi.portallog where day between %s and %s and busitype in(1,3) group by provinceid,busitype",
             "upsert into wifi.cityweekstat(id,year,week,busitype,au) select cityid,%s,%s,busitype,count(distinct usermac) from wifi.portallog where day between %s and %s and busitype in(1,3) group by cityid,busitype",
             "upsert into wifi.areaweekstat(id,year,week,busitype,au) select areaid,%s,%s,busitype,count(distinct usermac) from wifi.portallog where day between %s and %s and busitype in(1,3) group by areaid,busitype" };
    private static final String[] OTHER_SQLS =
            {
             "upsert into wifi.apweekstat(id,year,week,busitype,pv,nu,ru) select id,%s,%s,busitype,sum(dpv),sum(dnu),sum(dru) from wifi.apdaystat where day between %s and %s  group by id,busitype",
             "upsert into wifi.shopweekstat(id,year,week,busitype,pv,nu,ru) select id,%s,%s,busitype,sum(dpv),sum(dnu),sum(dru) from wifi.shopdaystat where day between %s and %s  group by id,busitype",
             "upsert into wifi.provinceweekstat(id,year,week,busitype,pv,nu,ru) select id,%s,%s,busitype,sum(dpv),sum(dnu),sum(dru) from wifi.provincedaystat where day between %s and %s  group by id,busitype",
             "upsert into wifi.cityweekstat(id,year,week,busitype,pv,nu,ru) select id,%s,%s,busitype,sum(dpv),sum(dnu),sum(dru) from wifi.citydaystat where day between %s and %s  group by id,busitype",
             "upsert into wifi.areaweekstat(id,year,week,busitype,pv,nu,ru) select id,%s,%s,busitype,sum(dpv),sum(dnu),sum(dru) from wifi.areadaystat where day between %s and %s  group by id,busitype" };


    /**
     * @param taskName
     *            Object
     * @param ownSign
     *            当前环境名称
     * @return
     * @throws Exception
     */
    public boolean execute(String taskName, String ownSign) {
        logger.info("{}任务执行开始", taskName);
        boolean execStatus = false;
        long start = System.currentTimeMillis();
        // 获取日期信息
        TimeUtil timeUtil = new TimeUtil();
        String preMonday = null;// 上一周星期一
        String preSunday = null;// 上一周星期日
        int year = 0;// 年份
        int week = 0;// 一年的第几个星期

        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        GregorianCalendar currentDate = new GregorianCalendar();

        for (int i = 0; i < backTime; i++) {
            preMonday = timeUtil.getPreviousWeekday("yyyyMMdd");
            try {
                currentDate.setTime(df.parse(preMonday));
                year = currentDate.get(Calendar.YEAR);
                week = currentDate.get(Calendar.WEEK_OF_YEAR);
            }
            catch (ParseException e) {
                e.printStackTrace();
            }
            currentDate.add(GregorianCalendar.DATE, 6);
            Date sunday = currentDate.getTime();
            preSunday = df.format(sunday);

            // 执行统计
            execStatus = execStat(taskName, connManager, year, week, preMonday, preSunday);
            logger.info("执行{}到{}的数据统计", preMonday, preSunday);
        }

        logger.info("{}任务执行结束,耗时{}", taskName, System.currentTimeMillis() - start);
        return execStatus;
    }


    private boolean execStat(String taskName, ConnectionManager connManager, int year, int week,
            String preMonday, String preSunday) {
        int today = Integer.parseInt(DateUtil.getNowTime(DateUtil.yyyymmdd));
        PhoenixConnection conn = connManager.getConn();
        try {
            PhoenixUtil.updateBatchStatement(conn, getExecSql(AU_SQLS, year, week, preMonday, preSunday));
            PhoenixUtil.updateBatchStatement(conn, getExecSql(OTHER_SQLS, year, week, preMonday, preSunday));
        }
        catch (SQLException e) {
            logger.error("执行任务{}{}周的数据统计失败,原因是{}", taskName, week, e.getMessage());
            e.printStackTrace();
            return false;
        }
        finally {
            connManager.closeConn(conn);
        }

        return true;
    }


    // 针对SQL语句进行转换
    public List<String> getExecSql(String[] sqls, int year, int week, String preMonday, String preSunday) {
        List<String> list = Lists.newArrayList();
        for (String sql : sqls) {
            list.add(String.format(sql, year, week, preMonday, preSunday));
        }
        return list;
    }


    public static void main(String[] args) {
        int backTime = 2;
        try {
            if (args.length > 0) {
                backTime = Integer.parseInt(args[0]);
            }

            WifiHistoryWeekStatSchedule task = new WifiHistoryWeekStatSchedule(backTime);
            task.execute(TASKNAME, "");
        }
        catch (Exception e) {
            logger.error("输入的参数要为数字");
            e.printStackTrace();
        }

    }
}