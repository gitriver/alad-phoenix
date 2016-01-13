package me.alad.phoenix;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import me.alad.phoenix.pool.ConnectionManager;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;


/**
 * 多线程进行测试 建表语句 create table demo(id varchar not null primary key,msg
 * varchar,time timestamp,seq unsigned_int);
 */
public class MutilThreadTest {
    public static void main(String[] args) throws InterruptedException, IOException {
        final ApplicationContext context =
                new ClassPathXmlApplicationContext("applicationContext-client.xml");

        ConnectionManager connManager = (ConnectionManager) context.getBean("connectionManagerBean");

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SS");
        System.out.println(sdf.format(new java.util.Date()));
        ExecutorService exec = Executors.newCachedThreadPool();
        for (int i = 0; i < 10; i++) {
            exec.execute(new ReadTask(i, connManager));
        }
        TimeUnit.SECONDS.sleep(120);
    }

}


class WriteTask implements Runnable {
    private Random rand = new Random(47);
    private int id;
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    Calendar now = GregorianCalendar.getInstance();
    ConnectionManager connManager;
    PhoenixConnection conn;


    public WriteTask(int id, ConnectionManager connManager) {
        this.id = id;
        this.connManager = connManager;
        conn = connManager.getConn();
    }


    public String getFormatNumber(int num) {
        NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(3);
        nf.setGroupingUsed(false);
        return nf.format(num);
    }


    @Override
    public void run() {
        System.out.println("thread " + id + " start");
        for (int i = 0; i < 10; i++) {
            now.add(Calendar.SECOND, 1);
            try {
                PhoenixUtil.execute(conn, "upsert into demo values(?,?,?,next value for myseq)", "id" + id
                        + getFormatNumber(i), "msg" + id + i, new Timestamp(now.getTimeInMillis()));
            }
            catch (SQLException e) {
                e.printStackTrace();
            }
        }
        connManager.closeConn(conn);
        System.out.println("thread " + id + " end");
    }
}


class ReadTask implements Runnable {
    private Random rand = new Random(47);
    private int id;
    ConnectionManager connManager;
    PhoenixConnection conn;


    public ReadTask(int id, ConnectionManager connManager) {
        this.id = id;
        this.connManager = connManager;
        conn = connManager.getConn();
    }


    @Override
    public void run() {
        System.out.println("thread " + id + " start");
        List<Map<String, Object>> list;
        try {
            String rowId = "id000" + id;
            list = PhoenixUtil.executeQuery(conn, "select * from demo where id=?", rowId);
            System.out.println(list);
        }
        catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        connManager.closeConn(conn);
        System.out.println("thread " + id + " end");
    }
}
