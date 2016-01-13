package me.alad.phoenix.datadeal;

//import org.joda.time.Days;
//import org.joda.time.Interval;
//import org.joda.time.Period;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtil {
	public static final String yyyyMMddHHmmss = "yyyy-MM-dd HH:mm:ss";
	public static final String yyyyMMddHH = "yyyyMMddHH";
	public static final String yyyyMMddHHmmssSS = "yyyy-MM-dd HH:mm:ss.SS";
	public static final String yyyyMMdd = "yyyy-MM-dd";
    public static final String yyyymmdd = "yyyyMMdd";

	public static Date parseToDate(String str, String style) {
		if ((str == null) || (str.length() < 8))
			return null;

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(style);
		try {
			return simpleDateFormat.parse(str);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static String parseToString(Date date) {
		return parseToString(date, yyyyMMddHHmmss);
	}

	public static String parseToString(Date date, String style) {
		if (date == null)
			return null;

		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(style);
		return simpleDateFormat.format(date);
	}

	public static Date parseLongToDate(long millsSeconds) {
		return new Date(millsSeconds);
	}

	public static String parseLongToString(long millsSeconds) {
		SimpleDateFormat df = new SimpleDateFormat(yyyyMMddHH);
		Date date = new Date(millsSeconds);
		return df.format(date);
	}

	public static String parseLongToString(long millsSeconds, String format) {
		SimpleDateFormat df = new SimpleDateFormat(format);
		Date date = new Date(millsSeconds);
		return df.format(date);
	}

	public static java.sql.Timestamp getNowDBDate() {
		return new java.sql.Timestamp(System.currentTimeMillis());
	}

	public static java.sql.Timestamp toDBDate(String str) {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
				yyyyMMddHHmmssSS);
		try {
			Date date = simpleDateFormat.parse(str);
			Calendar cal = Calendar.getInstance();
			cal.setTime(date);
			return new java.sql.Timestamp(cal.getTimeInMillis());
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static String getNowTime() {
		return getNowTime(yyyyMMddHHmmss);
	}

	public static String getShortNowTime() {
		return getNowTime(yyyyMMdd);
	}

	public static String getLongNowTime() {
		return getNowTime(yyyyMMddHHmmssSS);
	}

	public static String getNowTime(String format) {
		Date nowDate = new Date();
		Calendar now = Calendar.getInstance();
		now.setTime(nowDate);
		SimpleDateFormat formatter = new SimpleDateFormat(format);
		return formatter.format(now.getTime());
	}

	public static long getTwoTimeInterval(String startTime) {
		SimpleDateFormat df = new SimpleDateFormat(yyyyMMddHH);
		try {
			Date starDate = df.parse(startTime);
			return System.currentTimeMillis() - starDate.getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return 0;
	}

	public static long getTwoTimeInterval(String startTime, String endTime) {
		return getTwoTimeInterval(startTime, endTime, yyyyMMddHH);
	}

	public static long getTwoTimeInterval(String startTime, String endTime,
			String format) {
		SimpleDateFormat df = new SimpleDateFormat(format);
		try {
			Date starDate = df.parse(startTime);
			Date endDate = df.parse(endTime);
			return endDate.getTime() - starDate.getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return 0;
	}

	public static int getYear() {
		Calendar now = Calendar.getInstance();
		return now.get(Calendar.YEAR);
	}

	public static int getMonth() {
		Calendar now = Calendar.getInstance();
		return now.get(Calendar.MONTH);
	}

	public static int getDay() {
		Calendar now = Calendar.getInstance();
		return now.get(Calendar.DAY_OF_MONTH);
	}

	public static int getHours() {
		Calendar now = Calendar.getInstance();
		return now.get(Calendar.HOUR);
	}

	public static void main(String[] args) {
//		System.out.println(getTwoTimeInterval("2014061314", "2014061315"));
//		String str = getNowTime();
//		System.out.println(parseToString(toDBDate("2014-06-13 14:14:10.518"),
//				"yyyyMMddHH"));
//		Date logDate1 = DateUtil.toDBDate("2014-06-16 10:13:20.684");

//		System.out.println(DateUtil.parseToString(logDate1, yyyyMMddHHmmssSS));
//         long datatimec = 1419436800011L;
//        System.out.println(DateUtil.parseLongToString(1422581512147l,yyyyMMddHHmmssSS));
//        System.out.println(DateUtil.parseLongToString(1422522976911L,yyyyMMddHHmmssSS));
//        System.out.println(DateUtil.parseLongToString(1422522982941L,yyyyMMddHHmmssSS));
//        System.out.println(DateUtil.parseLongToString(1422524168593L,yyyyMMddHHmmssSS));
//        System.out.println(DateUtil.parseLongToString(1422524175067L,yyyyMMddHHmmssSS));


//		System.out.println(getYear() + "," + getMonth() + "," + getDay());
//      System.out.println(DateUtil.parseToDate("2014-12-29 15:00:00",yyyyMMddHHmmss).getTime());

//        System.out.println("20150130,20150131".split(",").length);
        String dateTime = "20150112";

//        Calendar cal = new GregorianCalendar();
//        cal.setTime(parseToDate(dateTime,yyyymmdd));
//
//        cal.add(Calendar.DATE, -1);
//        Date d = cal.getTime();
//        SimpleDateFormat sp = new SimpleDateFormat("yyyyMMdd");
//        String yesterday = sp.format(d);//获取昨天日期
//        System.out.println(yesterday);


//        Date date1 = DateUtil.parseToDate("20150112", DateUtil.yyyymmdd);
//        Date date2 = DateUtil.parseToDate("20150119" + "", DateUtil.yyyymmdd);
//
//        System.out.println(date1.getTime());
//        System.out.println(date2.getTime());
          String startDay = "20150228";
          String endDay = "20150301";
          long days = getTwoTimeInterval(startDay,endDay,yyyymmdd);

          System.out.println(days);
          System.out.println(days/(24*60*60*1000));


//        Days.daysBetween(date1,date2);
//        Interval interval = new Interval(date1.getTime(), date2.getTime());
//        Period period = interval.toPeriod();
//        int year = period.getYears(); //计算出中间相差的天数.
//        int days = period.getDays(); //计算出中间相差的天数.
//        System.out.println(year+" "+days);
	}
}