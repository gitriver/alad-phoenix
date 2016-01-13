package me.alad.phoenix.datadeal;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
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
public class WifiHistoryMonthStatSchedule {

    private static transient Logger logger = LoggerFactory.getLogger(WifiHistoryMonthStatSchedule.class);

    private static final String TASKNAME = "wifiMonthStatSchedule";
    private static ApplicationContext context = new ClassPathXmlApplicationContext(
        "classpath:applicationContext-client.xml");
    private static ConnectionManager connManager = (ConnectionManager) context
        .getBean("connectionManagerBean");

    private static final String[] AU_SQLS =
            {
             "upsert into wifi.apmonthstat(id,year,month,busitype,au) select apmac,%s,%s,busitype,count(distinct usermac) from wifi.portallog where day between %s and %s and busitype in(1,3) group by apmac,busitype",
             "upsert into wifi.shopmonthstat(id,year,month,busitype,au) select shopid,%s,%s,busitype,count(distinct usermac) from wifi.portallog where day between %s and %s and busitype in(1,3) group by shopid,busitype",
             "upsert into wifi.provincemonthstat(id,year,month,busitype,au) select provinceid,%s,%s,busitype,count(distinct usermac) from wifi.portallog where day between %s and %s and busitype in(1,3) group by provinceid,busitype",
             "upsert into wifi.citymonthstat(id,year,month,busitype,au) select cityid,%s,%s,busitype,count(distinct usermac) from wifi.portallog where day between %s and %s and busitype in(1,3) group by cityid,busitype",
             "upsert into wifi.areamonthstat(id,year,month,busitype,au) select areaid,%s,%s,busitype,count(distinct usermac) from wifi.portallog where day between %s and %s and busitype in(1,3) group by areaid,busitype" };
    private static final String[] OTHER_SQLS =
            {
             "upsert into wifi.apmonthstat(id,year,month,busitype,pv,nu,ru) select id,%s,%s,busitype,sum(dpv),sum(dnu),sum(dru) from wifi.apdaystat where day between %s and %s group by id,busitype",
             "upsert into wifi.shopmonthstat(id,year,month,busitype,pv,nu,ru) select id,%s,%s,busitype,sum(dpv),sum(dnu),sum(dru) from wifi.shopdaystat where day between %s and %s group by id,busitype",
             "upsert into wifi.provincemonthstat(id,year,month,busitype,pv,nu,ru) select id,%s,%s,busitype,sum(dpv),sum(dnu),sum(dru) from wifi.provincedaystat where day between %s and %s group by id,busitype",
             "upsert into wifi.citymonthstat(id,year,month,busitype,pv,nu,ru) select id,%s,%s,busitype,sum(dpv),sum(dnu),sum(dru) from wifi.citydaystat where day between %s and %s group by id,busitype",
             "upsert into wifi.areamonthstat(id,year,month,busitype,pv,nu,ru) select id,%s,%s,busitype,sum(dpv),sum(dnu),sum(dru) from wifi.areadaystat where day between %s and %s group by id,busitype" };

    private int backTime;


    public WifiHistoryMonthStatSchedule(int backTime) {
        this.backTime = backTime;
    }


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
        String preMonthFirst = null;
        String prevMonthEnd = null;

        Calendar ca = Calendar.getInstance();

        int year = 0;
        int month = 0;

        for (int i = 0; i < backTime; i++) {
            ca.add(Calendar.MONTH, -1); // 上个月，用于获取上月对应的年(跨年时)、月
            year = ca.get(Calendar.YEAR);
            month = ca.get(Calendar.MONTH) + 1;

            preMonthFirst = getMonthFirst(ca.getTime());
            prevMonthEnd = getMonthEnd(ca.getTime());

            logger.info("统计{}年{}月({}-{})的数据", year, month, preMonthFirst, prevMonthEnd);

            // 执行统计
            execStatus = execStat(taskName, connManager, year, month, preMonthFirst, prevMonthEnd);

        }

        logger.info("{}任务执行结束,耗时{}", taskName, System.currentTimeMillis() - start);
        return execStatus;
    }


    // 获取当前月第一天的日期
    private String getMonthFirst(Date date) {
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        Calendar lastDate = Calendar.getInstance();
        lastDate.setTime(date);
        lastDate.set(Calendar.DATE, 1);// 设为当前月的1号
        String preMonthFirst = df.format(lastDate.getTime());
        return preMonthFirst;

    }


    // 获取当前月最后一天的日期
    private String getMonthEnd(Date date) {
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        Calendar lastDate = Calendar.getInstance();
        lastDate.setTime(date);
        lastDate.set(Calendar.DATE, 1);// 把日期设置为当月第一天
        lastDate.roll(Calendar.DATE, -1);// 日期回滚一天，也就是本月最后一天
        String prevMonthEnd = df.format(lastDate.getTime());
        return prevMonthEnd;

    }


    private boolean execStat(String taskName, ConnectionManager connManager, int year, int month,
            String preMonthFirst, String preMonthEnd) {
        int today = Integer.parseInt(DateUtil.getNowTime(DateUtil.yyyymmdd));
        PhoenixConnection conn = connManager.getConn();
        try {
            PhoenixUtil.updateBatchStatement(conn,
                getExecSql(AU_SQLS, year, month, preMonthFirst, preMonthEnd));
            PhoenixUtil.updateBatchStatement(conn,
                getExecSql(OTHER_SQLS, year, month, preMonthFirst, preMonthEnd));
        }
        catch (SQLException e) {
            logger.error("执行任务{}{}月的数据统计失败,原因是{}", taskName, month, e.getMessage());
            e.printStackTrace();
            return false;
        }
        finally {
            connManager.closeConn(conn);
        }
        return true;
    }


    // 针对SQL语句进行转换
    public List<String> getExecSql(String[] sqls, int year, int month, String preMonthFirst,
            String preMonthEnd) {
        List<String> list = Lists.newArrayList();
        for (String sql : sqls) {
            list.add(String.format(sql, year, month, preMonthFirst, preMonthEnd));
        }
        return list;
    }


    public static void main(String[] args) {
        int backTime = 3;
        try {
            if (args.length > 0) {
                backTime = Integer.parseInt(args[0]);
            }

            WifiHistoryMonthStatSchedule task = new WifiHistoryMonthStatSchedule(backTime);
            task.execute(TASKNAME, "");
        }
        catch (Exception e) {
            logger.error("输入的参数要为数字");
            e.printStackTrace();
        }
    }
}