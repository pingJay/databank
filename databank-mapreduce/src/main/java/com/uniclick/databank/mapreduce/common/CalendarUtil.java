package com.uniclick.databank.mapreduce.common;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;

/**
 * @author yang qi
 * @date 20110831
 */

public class CalendarUtil {
	private int weeks = 0;// 用来全局控制 上一周，本周，下一周的周数变化
	private int MaxDate; // 一月最大天数
	private int MaxYear; // 一年最大天数

	/**
	 * 测试
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		CalendarUtil tt = new CalendarUtil();

		 System.out.println("根据输入的月数计算该月的第一天和最后一天的时间:"+tt.getLastWeekSomeDayByWeekNum(4));
		 System.out.println("根据输入的月数计算该月的第一天和最后一天的时间:"+tt.getLastWeekToThisWeek(4));
		//System.out.println(tt.getWeek(34));
		//System.out.println("根据输入的周数计算改周的周一和周日的时间:" + tt.getDateByInputWeek(52));
		// System.out.println("上个月是今年的第几月:"+tt.getPreMonthInYear());
		// System.out.println("这周是今年的第几周:"+tt.getWeekOfYear());
		//System.out.println("上周是今年的第几周:"+tt.getLastWeekOfYear());
		// System.out.println("请字符串的时间转换为Date类型的时间:"+tt.changDateByString("2011-08-31"));
		// System.out.println("获取当天日期String:" +
		// tt.getNowTimeString("yyyy-MM-dd"));
		// System.out.println("获得当天日期Date:"+tt.getNowTimeDate("yyyy-MM-dd"));
		// System.out.println("获取昨天的日期:"+tt.getYesterdayTime("yyyy-MM-dd"));
		// System.out.println("获取本周一日期:" + tt.getMondayOFWeek("yyyy-MM-dd"));
		// System.out.println("获取本周日的日期~:" + tt.getCurrentWeekday());
		// System.out.println("获取上周一日期:" + tt.getPreviousWeekday());
		// System.out.println("获取上周日日期:" + tt.getPreviousWeekSunday());
		// System.out.println("获取下周一日期:" + tt.getNextMonday());
		// System.out.println("获取下周日日期:" + tt.getNextSunday());
		// System.out.println("获得相应周的周六的日期:" + tt.getNowTime("yyyy-MM-dd"));
		// System.out.println("获取本月第一天日期:" + tt.getFirstDayOfMonth());
		// System.out.println("获取本月最后一天日期:" + tt.getDefaultDay());
		// System.out.println("获取上月第一天日期:" + tt.getPreviousMonthFirst());
		// System.out.println("获取上月最后一天的日期:" + tt.getPreviousMonthEnd());
		// System.out.println("获取下月第一天日期:" + tt.getNextMonthFirst());
		// System.out.println("获取下月最后一天日期:" + tt.getNextMonthEnd());
		// System.out.println("获取本年的第一天日期:" + tt.getCurrentYearFirst());
		// System.out.println("获取本年最后一天日期:" + tt.getCurrentYearEnd());
		// System.out.println("获取去年的第一天日期:" + tt.getPreviousYearFirst());
		// System.out.println("获取去年的最后一天日期:" + tt.getPreviousYearEnd());
		// System.out.println("获取明年第一天日期:" + tt.getNextYearFirst());
		// System.out.println("获取明年最后一天日期:" + tt.getNextYearEnd());
		// System.out.println("获取本季度第一天:" + tt.getThisSeasonFirstTime(11));
		// System.out.println("获取本季度最后一天:" + tt.getThisSeasonFinallyTime(11));
		// System.out.println("获取两个日期之间间隔天数2008-12-1~2008-9.29:"
		// + CalendarUtil.getTwoDay("2008-12-1", "2008-9-29"));
		// System.out.println("获取当前月的第几周：" + tt.getWeekOfMonth());
		// System.out.println("获取当前年份：" + tt.getYear());
		// System.out.println("获取当前月份：" + tt.getMonth());
		// System.out.println("获取今天在本年的第几天：" + tt.getDayOfYear());
		// System.out.println("获得今天在本月的第几天(获得当前日)：" + tt.getDayOfMonth());
		// System.out.println("获得今天在本周的第几天：" + tt.getDayOfWeek());
		// System.out.println("获得半年后的日期："
		// + tt.convertDateToString(tt.getTimeYearNext()));

		// System.out.println(tt.getPreWeekAllDate());
		// System.out.println(tt.getYearWeekMonth(45));
		// System.out.println(tt.judgeWeekDate("2011-11-14", "2011-11-20"));

	}

	/**
	 * 
	 * @param str
	 *            日期
	 * @return
	 * @function String 日期 转换为 Date 日期
	 */
	public static Date str2Date(String str) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		if (str == null)
			return null;

		try {
			return format.parse(str);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 
	 * @param date1
	 *            日期
	 * @param date2
	 *            日期
	 */
	public static String[] arrStartAndEndTime(String date1, String date2) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		String[] arr = null;
		if (date1.equals(date2)) {// 判断两日期是否相等
			arr = new String[1];
			arr[0] = date1;
			return arr;
		}

		String tmp;
		if (date1.compareTo(date2) > 0) { // 确保 date1的日期不晚于date2
			tmp = date1;
			date1 = date2;
			date2 = tmp;
		}
		long x = str2Date(date2).getTime() - str2Date(date1).getTime();
		int len = (int) ((str2Date(date2).getTime() - str2Date(date1).getTime()) / (3600 * 24 * 1000)) + 1;
		arr = new String[len];
		arr[0] = date1;
		tmp = format.format(str2Date(date1).getTime() + 3600 * 24 * 1000);// 起始日期+1DAY

		int num = 0;
		while (tmp.compareTo(date2) < 0) {
			num++;
			arr[num] = tmp;
			tmp = format.format(str2Date(tmp).getTime() + 3600 * 24 * 1000);
		}
		arr[len - 1] = date2;
		return arr;
	}

	/**
	 * 获得当前年份
	 * 
	 * @return
	 */
	public static int getYear() {
		return Calendar.getInstance().get(Calendar.YEAR);
	}

	/**
	 * 取得年周 或 年月
	 * 
	 * @param input
	 * @return
	 */
	public static int getYearWeekMonth(int input) {
		int year = Calendar.getInstance().get(Calendar.YEAR);
		;
		if ((String.valueOf(input)).length() == 1) {
			return Integer.valueOf(String.valueOf(year) + "0"
					+ String.valueOf(input));
		} else {
			return Integer
					.valueOf(String.valueOf(year) + String.valueOf(input));
		}

	}

	/**
	 * 获得当前月份
	 * 
	 * @return
	 */
	public static int getMonth() {
		return Calendar.getInstance().get(Calendar.MONTH) + 1;
	}

	/**
	 * 获得今天在本年的第几天
	 * 
	 * @return
	 */
	public static int getDayOfYear() {
		return Calendar.getInstance().get(Calendar.DAY_OF_YEAR);
	}

	/**
	 * 这周是今年的第几周
	 */
	public int getWeekOfYear() {
		return Calendar.getInstance().get(Calendar.WEEK_OF_YEAR);
	}

	/**
	 * 获得上周是这一年的第几周
	 */
	public int getLastWeekOfYear() {
		int week = -1;
		int mondayPlus = this.getMondayPlus();
		GregorianCalendar currentDate = new GregorianCalendar();
		currentDate.add(GregorianCalendar.DATE, mondayPlus + 7 * week);
		Date monday = currentDate.getTime();
		Calendar cal = Calendar.getInstance();
		cal.setTime(monday);
		return cal.get(Calendar.WEEK_OF_YEAR);
	}

	/**
	 * 获得今天在本月的第几天(获得当前日)
	 * 
	 * @return
	 */
	public static int getDayOfMonth() {
		return Calendar.getInstance().get(Calendar.DAY_OF_MONTH);
	}

	/**
	 * 获得今天在本周的第几天
	 * 
	 * @return
	 */
	public static int getDayOfWeek() {
		Calendar cal = Calendar.getInstance();
		String str = "2011-08-27";
		SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd");
		Date date = new Date();
		try {
			date = sf.parse(str);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		cal.setTime(date);
		return cal.get(Calendar.DAY_OF_WEEK);
	}

	/**
	 * 获得今天是这个月的第几周
	 * 
	 * @return
	 */
	public static int getWeekOfMonth() {
		return Calendar.getInstance().get(Calendar.DAY_OF_WEEK_IN_MONTH);
	}

	/**
	 * 获得半年后的日期
	 * 
	 * @return
	 */
	public static Date getTimeYearNext() {
		Calendar.getInstance().add(Calendar.DAY_OF_YEAR, 183);
		return Calendar.getInstance().getTime();
	}

	/**
	 * 将日期转换成字符串
	 * 
	 * @param dateTime
	 * @return
	 */
	public static String convertDateToString(Date dateTime) {
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		return df.format(dateTime);
	}

	/**
	 * 得到二个日期间的间隔天数
	 * 
	 * @param sj1
	 * @param sj2
	 * @return
	 */
	public static String getTwoDay(String sj1, String sj2) {
		SimpleDateFormat myFormatter = new SimpleDateFormat("yyyy-MM-dd");
		long day = 0;
		try {
			java.util.Date date = myFormatter.parse(sj1);
			java.util.Date mydate = myFormatter.parse(sj2);
			day = (date.getTime() - mydate.getTime()) / (24 * 60 * 60 * 1000);
		} catch (Exception e) {
			return "";
		}
		return day + "";
	}

	/**
	 * 根据一个日期，返回是星期几的字符串
	 * 
	 * @param sdate
	 * @return
	 */
	public static String getWeek(String sdate) {
		// 再转换为时间
		Date date = CalendarUtil.strToDate(sdate);
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		// int hour=c.get(Calendar.DAY_OF_WEEK);
		// hour中存的就是星期几了，其范围 1~7
		// 1=星期日 7=星期六，其他类推
		return new SimpleDateFormat("EEEE").format(c.getTime());
	}

	/**
	 * 将短时间格式字符串转换为时间 yyyy-MM-dd
	 * 
	 * @param strDate
	 * @return
	 */
	public static Date strToDate(String strDate) {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		ParsePosition pos = new ParsePosition(0);
		Date strtodate = formatter.parse(strDate, pos);
		return strtodate;
	}

	/**
	 * 两个时间之间的天数
	 * 
	 * @param date1
	 * @param date2
	 * @return
	 */
	public static long getDays(String date1, String date2) {
		if (date1 == null || date1.equals(""))
			return 0;
		if (date2 == null || date2.equals(""))
			return 0;
		// 转换为标准时间
		SimpleDateFormat myFormatter = new SimpleDateFormat("yyyy-MM-dd");
		java.util.Date date = null;
		java.util.Date mydate = null;
		try {
			date = myFormatter.parse(date1);
			mydate = myFormatter.parse(date2);
		} catch (Exception e) {
		}
		long day = (date.getTime() - mydate.getTime()) / (24 * 60 * 60 * 1000);
		return day;
	}

	/**
	 * 计算当月最后一天,返回字符串
	 * 
	 * @return
	 */
	public String getDefaultDay() {
		String str = "";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

		Calendar lastDate = Calendar.getInstance();
		lastDate.set(Calendar.DATE, 1);// 设为当前月的1号
		lastDate.add(Calendar.MONTH, 1);// 加一个月，变为下月的1号
		lastDate.add(Calendar.DATE, -1);// 减去一天，变为当月最后一天
		str = sdf.format(lastDate.getTime());
		return str;
	}

	/**
	 * 上个月是今年的第几个月
	 */

	public int getPreMonthInYear() {
		String str = "";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

		Calendar lastDate = Calendar.getInstance();
		lastDate.set(Calendar.DATE, 1);// 设为当前月的1号
		lastDate.add(Calendar.MONTH, -1);// 减一个月，变为下月的1号
		// lastDate.add(Calendar.DATE,-1);//减去一天，变为当月最后一天

		str = sdf.format(lastDate.getTime());

		Calendar cal = Calendar.getInstance();
		cal.setTime(lastDate.getTime());
		return cal.get(Calendar.MONTH) + 1;
	}

	/**
	 * 上月第一天
	 * 
	 * @return
	 */
	public String getPreviousMonthFirst() {
		String str = "";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Calendar lastDate = Calendar.getInstance();
		lastDate.set(Calendar.DATE, 1);// 设为当前月的1号
		lastDate.add(Calendar.MONTH, -1);// 减一个月，变为下月的1号
		// lastDate.add(Calendar.DATE,-1);//减去一天，变为当月最后一天

		str = sdf.format(lastDate.getTime());
		return str;
	}

	/**
	 * 获取当月第一天
	 * 
	 * @return
	 */
	public String getFirstDayOfMonth() {
		String str = "";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

		Calendar lastDate = Calendar.getInstance();
		lastDate.set(Calendar.DATE, 1);// 设为当前月的1号
		str = sdf.format(lastDate.getTime());
		return str;
	}

	/**
	 * 根据输入的第几月来计算出几个月的第一天的日期和最后一天的日期
	 */
	public Map<String, Object> getDateByMonth(int month) {
		Map<String, Object> map = new HashMap<String, Object>();
		int year = Calendar.getInstance().get(Calendar.YEAR);
		String strYear = String.valueOf(year);
		String strMonth = String.valueOf(month);
		String compareDate = strYear + "-" + strMonth + "-" + "01";

		SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd");
		Date date = new Date();
		try {
			date = sf.parse(compareDate);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		/***
		 * 获得该月的第一天
		 */
		Calendar cal = Calendar.getInstance();
		;
		cal.setTime(date);
		cal.set(Calendar.DATE, 1);// 设为当前月的1号
		String str = sf.format(cal.getTime());
		map.put("beginTime", str);
		/**
		 * 获得该月的最后一天
		 */
		Calendar lastDate = Calendar.getInstance();
		lastDate.setTime(date);
		lastDate.set(Calendar.DATE, 1);// 设为当前月的1号
		lastDate.add(Calendar.MONTH, 1);// 加一个月，变为下月的1号
		lastDate.add(Calendar.DATE, -1);// 减去一天，变为当月最后一天
		String str1 = sf.format(lastDate.getTime());
		map.put("endTime", str1);
		return map;
	}

	/**
	 * 获得本周星期日的日期
	 * 
	 * @return
	 */
	public String getCurrentWeekday() {
		weeks = 0;
		int mondayPlus = this.getMondayPlus();
		GregorianCalendar currentDate = new GregorianCalendar();
		currentDate.add(GregorianCalendar.DATE, mondayPlus + 6);
		Date monday = currentDate.getTime();

		DateFormat df = DateFormat.getDateInstance();
		String preMonday = df.format(monday);
		return preMonday;
	}

	/**
	 * 获取当天时间String
	 * 
	 * @param dateformat
	 * @return
	 */
	public String getNowTimeString(String dateformat) {
		Date now = new Date();
		SimpleDateFormat dateFormat = new SimpleDateFormat(dateformat);// 可以方便地修改日期格式
		String hehe = dateFormat.format(now);
		return hehe;
	}

	/**
	 * 获得当天时间Date
	 */
	public Date getNowTimeDate(String dateformat) {
		Date date = new Date();
		SimpleDateFormat sf = new SimpleDateFormat(dateformat);
		String str = sf.format(date);
		Date changeDate = new Date();
		try {
			changeDate = sf.parse(str);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return changeDate;
	}

	/**
	 * 获取昨天的数据
	 */

	public String getYesterdayTime(String dateformat) {
		Date date = new Date();
		SimpleDateFormat sf = new SimpleDateFormat(dateformat);
		long j = date.getTime();
		j = j - 1000 * 60 * 60 * 24;
		Date date1 = new Date(j);
		String strDate = sf.format(date1);
		return strDate;
	}
	
	public String getDateTimeToFormatString(Date date,String format){
		if(null == date)
			return null;
		SimpleDateFormat sf = new SimpleDateFormat(format);
		String strDate = sf.format(date);
		return strDate;
	}

	/**
	 * 获得当前日期与本周日相差的天数
	 * 
	 * @return
	 */
//	private int getMondayPlus() {
		public int getMondayPlus() {
		Calendar cd = Calendar.getInstance();
		// 获得今天是一周的第几天，星期日是第一天，星期二是第二天......
		int dayOfWeek = cd.get(Calendar.DAY_OF_WEEK) - 1; // 因为按中国礼拜一作为第一天所以这里减1
		if (dayOfWeek == 1) {
			return 0;
		} else {
			return 1 - dayOfWeek;
		}
	}

	/**
	 * 获得本周一的日期
	 * 
	 * @return
	 */
	public String getMondayOFWeek(String format) {
		weeks = 0;
		int mondayPlus = this.getMondayPlus();
		GregorianCalendar currentDate = new GregorianCalendar();
		currentDate.add(GregorianCalendar.DATE, mondayPlus);
		Date monday = currentDate.getTime();

		SimpleDateFormat sf = new SimpleDateFormat(format);
		String preMonday = sf.format(monday);
		return preMonday;
	}

	/**
	 * 获得相应周的周六的日期
	 * 注意 本方法有误
	 * @return
	 */
	public String getSaturday() {
		int mondayPlus = this.getMondayPlus();
		GregorianCalendar currentDate = new GregorianCalendar();
		currentDate.add(GregorianCalendar.DATE, mondayPlus + 7 * weeks + 6);
		Date monday = currentDate.getTime();
		DateFormat df = DateFormat.getDateInstance();
		String preMonday = df.format(monday);
		return preMonday;
	}
	
	/**
	 * 获得本周？的日期
	 * @author zhou yuanlong
	 * @param 1为本周一  ...  7为本周日
	 * @return
	 */
	public String getWeekSomeDayByWeekNum(int weekNum) {
		weeks = 0;
		weeks--;
		int mondayPlus = this.getMondayPlus();
		if(mondayPlus == 1){
			mondayPlus = mondayPlus - 7;
		}
		GregorianCalendar currentDate = new GregorianCalendar();//(7 - weekNum + 1) * weeks +7
//		currentDate.add(GregorianCalendar.DATE, mondayPlus + 7 * weeks + (weekNum - 1));
//		currentDate.add(GregorianCalendar.DATE, mondayPlus + (7 - weekNum + 1) * weeks +7);
		currentDate.add(GregorianCalendar.DATE, mondayPlus + (7 - weekNum + 1) * weeks +7);
		Date monday = currentDate.getTime();
//		DateFormat df = DateFormat.getDateInstance();
		SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd");
		String preMonday = sf.format(monday);
		return preMonday;
	}
	

	/**
	 * 获得上周星期日的日期
	 * 
	 * @return
	 */
	public String getPreviousWeekSunday() {
		weeks = 0;
		weeks--;
		int mondayPlus = this.getMondayPlus();
		GregorianCalendar currentDate = new GregorianCalendar();
		currentDate.add(GregorianCalendar.DATE, mondayPlus + weeks);
		Date monday = currentDate.getTime();
		SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd");
		// DateFormat df = DateFormat.getDateInstance();
		String preMonday = sf.format(monday);
		return preMonday;
	}

	/**
	 * 获得上周星期一的日期
	 * 
	 * @return
	 */
	public String getPreviousWeekday() {
		weeks = 0;
		weeks--;
		int mondayPlus = this.getMondayPlus();
		GregorianCalendar currentDate = new GregorianCalendar();
		currentDate.add(GregorianCalendar.DATE, mondayPlus + 7 * weeks);
		Date monday = currentDate.getTime();
		SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd");
		// DateFormat df = DateFormat.getDateInstance();
		String preMonday = sf.format(monday);
		return preMonday;
	}
	/**
	 * 获得上周星期二日期
	 * 
	 * @return
	 */
	public String getPreviousTuesday() {
		weeks = 0;
		weeks--;
		int mondayPlus = this.getMondayPlus();
		GregorianCalendar currentDate = new GregorianCalendar();
		currentDate.add(GregorianCalendar.DATE, mondayPlus + 6 * weeks);
		Date monday = currentDate.getTime();
		SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd");
		// DateFormat df = DateFormat.getDateInstance();
		String preMonday = sf.format(monday);
		return preMonday;
	}
	/**
	 * 获得上周星期三的日期
	 * 
	 * @return
	 */
	public String getPreviousWednesday() {
		weeks = 0;
		weeks--;
		int mondayPlus = this.getMondayPlus();
		GregorianCalendar currentDate = new GregorianCalendar();
		currentDate.add(GregorianCalendar.DATE, mondayPlus + 5 * weeks);
		Date monday = currentDate.getTime();
		SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd");
		// DateFormat df = DateFormat.getDateInstance();
		String preMonday = sf.format(monday);
		return preMonday;
	}
	/**
	 * 获得上周星期四的日期
	 * 
	 * @return
	 */
	public String getPreviousThursday() {
		weeks = 0;
		weeks--;
		int mondayPlus = this.getMondayPlus();
		GregorianCalendar currentDate = new GregorianCalendar();
		currentDate.add(GregorianCalendar.DATE, mondayPlus + 4 * weeks);
		Date monday = currentDate.getTime();
		SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd");
		// DateFormat df = DateFormat.getDateInstance();
		String preMonday = sf.format(monday);
		return preMonday;
	}
	/**
	 * 获得上周星期五的日期
	 * 
	 * @return
	 */
	public String getPreviousFriday() {
		weeks = 0;
		weeks--;
		int mondayPlus = this.getMondayPlus();
		GregorianCalendar currentDate = new GregorianCalendar();
		currentDate.add(GregorianCalendar.DATE, mondayPlus + 3 * weeks);
		Date monday = currentDate.getTime();
		SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd");
		// DateFormat df = DateFormat.getDateInstance();
		String preMonday = sf.format(monday);
		return preMonday;
	}
	/**
	 * 获得上周星期六的日期
	 * 
	 * @return
	 */
	public String getPreviousSaturday() {
		weeks = 0;
		weeks--;
		int mondayPlus = this.getMondayPlus();
		GregorianCalendar currentDate = new GregorianCalendar();
		currentDate.add(GregorianCalendar.DATE, mondayPlus + 2 * weeks);
		Date monday = currentDate.getTime();
		SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd");
		// DateFormat df = DateFormat.getDateInstance();
		String preMonday = sf.format(monday);
		return preMonday;
	}
	
	/**
	 * 获得上周？是几号
	 * @param 根据传入的星期的编号  0 无,1 周一,2 周二 ...,7 周日
	 * @return
	 */
	public String getLastWeekSomeDayByWeekNum(int weekNum) {
		weeks = 0;
		weeks--;
		int mondayPlus = this.getMondayPlus();
		GregorianCalendar currentDate = new GregorianCalendar();
		currentDate.add(GregorianCalendar.DATE, mondayPlus + (7 - weekNum + 1) * weeks );
		Date monday = currentDate.getTime();
		SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd");
		// DateFormat df = DateFormat.getDateInstance();
		String preMonday = sf.format(monday);
		return preMonday;
	}
	
	/**
	 * 获得指定天数后面的7天（如上周四到本周三，共7天）
	 * 获得上周某天后的第7天
	 * @param 根据传入的星期的编号  0 无,1 周一,2 周二 ...,7 周日
	 * @return
	 */
	public String getLastWeekToThisWeek(int weekNum) {
		weeks = 0;
		weeks--;
		int mondayPlus = this.getMondayPlus();
		GregorianCalendar currentDate = new GregorianCalendar();
		currentDate.add(GregorianCalendar.DATE, mondayPlus + (7 - weekNum + 1) * weeks + 6);
		Date monday = currentDate.getTime();
		SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd");
		// DateFormat df = DateFormat.getDateInstance();
		String preMonday = sf.format(monday);
		return preMonday;
	}

	/**
	 * 获得下周星期一的日期
	 */
	public String getNextMonday() {
		weeks++;
		int mondayPlus = this.getMondayPlus();
		GregorianCalendar currentDate = new GregorianCalendar();
		currentDate.add(GregorianCalendar.DATE, mondayPlus + 7);
		Date monday = currentDate.getTime();
		DateFormat df = DateFormat.getDateInstance();
		String preMonday = df.format(monday);
		return preMonday;
	}

	/**
	 * 获得下周星期日的日期
	 */
	public String getNextSunday() {

		int mondayPlus = this.getMondayPlus();
		GregorianCalendar currentDate = new GregorianCalendar();
		currentDate.add(GregorianCalendar.DATE, mondayPlus + 7 + 6);
		Date monday = currentDate.getTime();
		DateFormat df = DateFormat.getDateInstance();
		String preMonday = df.format(monday);
		return preMonday;
	}

	private int getMonthPlus() {
		Calendar cd = Calendar.getInstance();
		int monthOfNumber = cd.get(Calendar.DAY_OF_MONTH);
		cd.set(Calendar.DATE, 1);// 把日期设置为当月第一天
		cd.roll(Calendar.DATE, -1);// 日期回滚一天，也就是最后一天
		MaxDate = cd.get(Calendar.DATE);
		if (monthOfNumber == 1) {
			return -MaxDate;
		} else {
			return 1 - monthOfNumber;
		}
	}

	/**
	 * 获得上月最后一天的日期
	 * 
	 * @return
	 */
	public String getPreviousMonthEnd() {
		String str = "";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

		Calendar lastDate = Calendar.getInstance();
		lastDate.add(Calendar.MONTH, -1);// 减一个月
		lastDate.set(Calendar.DATE, 1);// 把日期设置为当月第一天
		lastDate.roll(Calendar.DATE, -1);// 日期回滚一天，也就是本月最后一天
		str = sdf.format(lastDate.getTime());
		return str;
	}

	/**
	 * 获得下个月第一天的日期
	 * 
	 * @return
	 */
	public String getNextMonthFirst() {
		String str = "";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

		Calendar lastDate = Calendar.getInstance();
		lastDate.add(Calendar.MONTH, 1);// 减一个月
		lastDate.set(Calendar.DATE, 1);// 把日期设置为当月第一天
		str = sdf.format(lastDate.getTime());
		return str;
	}

	/**
	 * 获得下个月最后一天的日期
	 * 
	 * @return
	 */
	public String getNextMonthEnd() {
		String str = "";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

		Calendar lastDate = Calendar.getInstance();
		lastDate.add(Calendar.MONTH, 1);// 加一个月
		lastDate.set(Calendar.DATE, 1);// 把日期设置为当月第一天
		lastDate.roll(Calendar.DATE, -1);// 日期回滚一天，也就是本月最后一天
		str = sdf.format(lastDate.getTime());
		return str;
	}

	/**
	 * 获得明年最后一天的日期
	 * 
	 * @return
	 */
	public String getNextYearEnd() {
		String str = "";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

		Calendar lastDate = Calendar.getInstance();
		lastDate.add(Calendar.YEAR, 1);// 加一个年
		lastDate.set(Calendar.DAY_OF_YEAR, 1);
		lastDate.roll(Calendar.DAY_OF_YEAR, -1);
		str = sdf.format(lastDate.getTime());
		return str;
	}

	/**
	 * 获得明年第一天的日期
	 * 
	 * @return
	 */
	public String getNextYearFirst() {
		String str = "";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

		Calendar lastDate = Calendar.getInstance();
		lastDate.add(Calendar.YEAR, 1);// 加一个年
		lastDate.set(Calendar.DAY_OF_YEAR, 1);
		str = sdf.format(lastDate.getTime());
		return str;

	}

	/**
	 * 获得本年有多少天
	 * 
	 * @return
	 */
	private int getMaxYear() {
		Calendar cd = Calendar.getInstance();
		cd.set(Calendar.DAY_OF_YEAR, 1);// 把日期设为当年第一天
		cd.roll(Calendar.DAY_OF_YEAR, -1);// 把日期回滚一天。
		int MaxYear = cd.get(Calendar.DAY_OF_YEAR);
		return MaxYear;
	}

	private int getYearPlus() {
		Calendar cd = Calendar.getInstance();
		int yearOfNumber = cd.get(Calendar.DAY_OF_YEAR);// 获得当天是一年中的第几天
		cd.set(Calendar.DAY_OF_YEAR, 1);// 把日期设为当年第一天
		cd.roll(Calendar.DAY_OF_YEAR, -1);// 把日期回滚一天。
		int MaxYear = cd.get(Calendar.DAY_OF_YEAR);
		if (yearOfNumber == 1) {
			return -MaxYear;
		} else {
			return 1 - yearOfNumber;
		}
	}

	/**
	 * 获得本年第一天的日期
	 * 
	 * @return
	 */
	public String getCurrentYearFirst() {
		int yearPlus = this.getYearPlus();
		GregorianCalendar currentDate = new GregorianCalendar();
		currentDate.add(GregorianCalendar.DATE, yearPlus);
		Date yearDay = currentDate.getTime();
		DateFormat df = DateFormat.getDateInstance();
		String preYearDay = df.format(yearDay);
		return preYearDay;
	}

	// 获得本年最后一天的日期 *
	public String getCurrentYearEnd() {
		Date date = new Date();
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy");// 可以方便地修改日期格式
		String years = dateFormat.format(date);
		return years + "-12-31";
	}

	// 获得上年第一天的日期 *
	public String getPreviousYearFirst() {
		Date date = new Date();
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy");// 可以方便地修改日期格式
		String years = dateFormat.format(date);
		int years_value = Integer.parseInt(years);
		years_value--;
		return years_value + "-1-1";
	}

	// 获得上年最后一天的日期
	public String getPreviousYearEnd() {
		weeks = 0;
		weeks--;
		int yearPlus = this.getYearPlus();
		GregorianCalendar currentDate = new GregorianCalendar();
		currentDate.add(GregorianCalendar.DATE, yearPlus + MaxYear * weeks
				+ (MaxYear - 1));
		Date yearDay = currentDate.getTime();
		DateFormat df = DateFormat.getDateInstance();
		String preYearDay = df.format(yearDay);
		return preYearDay;
	}

	/**
	 * 获得本季度第一天
	 * 
	 * @param month
	 * @return
	 */
	public String getThisSeasonFirstTime(int month) {
		int array[][] = { { 1, 2, 3 }, { 4, 5, 6 }, { 7, 8, 9 }, { 10, 11, 12 } };
		int season = 1;
		if (month >= 1 && month <= 3) {
			season = 1;
		}
		if (month >= 4 && month <= 6) {
			season = 2;
		}
		if (month >= 7 && month <= 9) {
			season = 3;
		}
		if (month >= 10 && month <= 12) {
			season = 4;
		}
		int start_month = array[season - 1][0];
		int end_month = array[season - 1][2];

		Date date = new Date();
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy");// 可以方便地修改日期格式
		String years = dateFormat.format(date);
		int years_value = Integer.parseInt(years);

		int start_days = 1;// years+"-"+String.valueOf(start_month)+"-1";//getLastDayOfMonth(years_value,start_month);
		int end_days = getLastDayOfMonth(years_value, end_month);
		String seasonDate = years_value + "-" + start_month + "-" + start_days;
		return seasonDate;

	}

	/**
	 * 获得本季度最后一天
	 * 
	 * @param month
	 * @return
	 */
	public String getThisSeasonFinallyTime(int month) {
		int array[][] = { { 1, 2, 3 }, { 4, 5, 6 }, { 7, 8, 9 }, { 10, 11, 12 } };
		int season = 1;
		if (month >= 1 && month <= 3) {
			season = 1;
		}
		if (month >= 4 && month <= 6) {
			season = 2;
		}
		if (month >= 7 && month <= 9) {
			season = 3;
		}
		if (month >= 10 && month <= 12) {
			season = 4;
		}
		int start_month = array[season - 1][0];
		int end_month = array[season - 1][2];

		Date date = new Date();
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy");// 可以方便地修改日期格式
		String years = dateFormat.format(date);
		int years_value = Integer.parseInt(years);

		int start_days = 1;// years+"-"+String.valueOf(start_month)+"-1";//getLastDayOfMonth(years_value,start_month);
		int end_days = getLastDayOfMonth(years_value, end_month);
		String seasonDate = years_value + "-" + end_month + "-" + end_days;
		return seasonDate;

	}
	
	/**
	 * 判断某天是属于哪个季度
	 * @return 季度一、二、三、四 分别用数字代替 1 2 3 4
	 */
	public int getQuarterNum(String day) {
		int month = 1;
//		day.substring(beginIndex, endIndex)
//		month = Integer.parseInt("2013-01-05".substring(5, 7));
		month = Integer.parseInt(day.substring(5, 7));
		int quarterNum = 1;
		if (month >= 1 && month <= 3) {
			quarterNum = 1;
		}
		if (month >= 4 && month <= 6) {
			quarterNum = 2;
		}
		if (month >= 7 && month <= 9) {
			quarterNum = 3;
		}
		if (month >= 10 && month <= 12) {
			quarterNum = 4;
		}
		
		return quarterNum;
	}
	
	

	/**
	 * 获取某年某月的最后一天
	 * 
	 * @param year
	 *            年
	 * @param month
	 *            月
	 * @return 最后一天
	 */
	private int getLastDayOfMonth(int year, int month) {
		if (month == 1 || month == 3 || month == 5 || month == 7 || month == 8
				|| month == 10 || month == 12) {
			return 31;
		}
		if (month == 4 || month == 6 || month == 9 || month == 11) {
			return 30;
		}
		if (month == 2) {
			if (isLeapYear(year)) {
				return 29;
			} else {
				return 28;
			}
		}
		return 0;
	}

	/**
	 * 是否闰年
	 * 
	 * @param year
	 *            年
	 * @return
	 */
	public boolean isLeapYear(int year) {
		return (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
	}

	/**
	 * 是否闰年
	 * 
	 * @param year
	 * @return
	 */
	public boolean isLeapYear2(int year) {
		return new GregorianCalendar().isLeapYear(year);
	}

	/**
	 * 请字符串的时间转换为Date类型的时间
	 * 
	 * @param strDate
	 * @return
	 */
	public Date changDateByString(String strDate) {
		SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd");
		Date date = new Date();
		try {
			date = sf.parse(strDate);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return date;
	}

	/**
	 * 根据传入的周数计算出此周的周一到周日的日期
	 */
	public String getWeek(int whichWeek) {
		int year = Calendar.getInstance().get(Calendar.YEAR);
		int inputWeek = whichWeek;
		Calendar calFirstDayOfTheYear = new GregorianCalendar(year,

		Calendar.JANUARY, 1);

		calFirstDayOfTheYear.add(Calendar.DATE, 7 * (inputWeek - 1));
		int dayOfWeek = calFirstDayOfTheYear.get(Calendar.DAY_OF_WEEK);

		Calendar calFirstDayInWeek = (Calendar) calFirstDayOfTheYear.clone();

		calFirstDayInWeek
				.add(Calendar.DATE,

				calFirstDayOfTheYear.getActualMinimum(Calendar.DAY_OF_WEEK)
						- dayOfWeek);

		Date firstDayInWeek = calFirstDayInWeek.getTime();

		long i = firstDayInWeek.getTime();
		//i = i + 1000 * 60 * 60 * 24;
		//Date date1 = new Date(i);
		SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd");
		//String monday = sf.format(date1);
		StringBuffer sb = new StringBuffer();
		for(int z=0 ; z<7 ; z++){
			i+=1000 * 60 * 60 * 24;
			if(z==6){
				sb.append(sf.format(new Date(i)));
			}else {
				sb.append(sf.format(new Date(i))).append(",");
			}
		}
		// System.out.println(year + "年第" + inputWeek + "个礼拜的最后一天是" + str2);
		return sb.toString();
	}

	/**
	 * 根据传入的周数计算出此周的周一和周日的日期
	 */
	public Map<String, Object> getDateByInputWeek(int whichWeek) {
		Map<String, Object> map = new HashMap<String, Object>();
		int year = Calendar.getInstance().get(Calendar.YEAR);
		int inputWeek = whichWeek;
		Calendar calFirstDayOfTheYear = new GregorianCalendar(year,

		Calendar.JANUARY, 1);

		calFirstDayOfTheYear.add(Calendar.DATE, 7 * (inputWeek - 1));
		int dayOfWeek = calFirstDayOfTheYear.get(Calendar.DAY_OF_WEEK);

		Calendar calFirstDayInWeek = (Calendar) calFirstDayOfTheYear.clone();

		calFirstDayInWeek
				.add(Calendar.DATE,

				calFirstDayOfTheYear.getActualMinimum(Calendar.DAY_OF_WEEK)
						- dayOfWeek);

		Date firstDayInWeek = calFirstDayInWeek.getTime();

		long i = firstDayInWeek.getTime();
		i = i + 1000 * 60 * 60 * 24;
		Date date1 = new Date(i);
		SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd");
		String str = sf.format(date1);

		// System.out.println(year + "年第" + inputWeek + "个礼拜的第一天是" + str);

		Calendar calLastDayInWeek = (Calendar) calFirstDayOfTheYear.clone();

		calLastDayInWeek
				.add(Calendar.DATE,

				calFirstDayOfTheYear.getActualMaximum(Calendar.DAY_OF_WEEK)
						- dayOfWeek);

		Date lastDayInWeek = calLastDayInWeek.getTime();

		long j = lastDayInWeek.getTime();
		j = j + 1000 * 60 * 60 * 24;
		Date date2 = new Date(j);
		String str2 = sf.format(date2);

		map.put("beginTime", str);
		map.put("endTime", str2);

		// System.out.println(year + "年第" + inputWeek + "个礼拜的最后一天是" + str2);
		return map;
	}

	public String getPreWeekAllDate() {
		weeks = 0;
		weeks--;
		int mondayPlus = this.getMondayPlus();
		String[] arr = new String[7];
		for (int i = 1, y = 6; i <= 7; i++, y--) {
			GregorianCalendar currentDate = new GregorianCalendar();
			currentDate.add(GregorianCalendar.DATE, mondayPlus + i * weeks);
			Date monday = currentDate.getTime();
			SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd");
			String preMonday = sf.format(monday);
			arr[y] = preMonday;
		}
		StringBuffer sb = new StringBuffer();
		sb.append("{");
		int len = arr.length;
		for (int i = 0; i < len; i++) {
			if (i < (len - 1)) {
				sb.append(arr[i]).append(",");
			} else {
				sb.append(arr[i]).append("}");
			}
		}
		String str = sb.toString();
		return str;
	}

	public boolean judgeWeekDate(String beginTime, String endTime) {
		SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd");
		long b = 0;
		try {
			b = sf.parse(beginTime).getTime();
		} catch (ParseException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		long e = 0;
		try {
			e = sf.parse(endTime).getTime();
		} catch (ParseException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		long c = 1000 * 60 * 60 * 24 * 6;
		long r = e - b;
		if (r == c) {
			return true;
		}

		return false;
	}

	/**
	 * 根据传入的周数计算出此周的周一和周日的日期
	 */
	public String getMonDayAndSunDayByWeek(int whichWeek) {
		Map<String, Object> map = new HashMap<String, Object>();
		int year = Calendar.getInstance().get(Calendar.YEAR);
		int inputWeek = whichWeek;
		Calendar calFirstDayOfTheYear = new GregorianCalendar(year,

		Calendar.JANUARY, 1);

		calFirstDayOfTheYear.add(Calendar.DATE, 7 * (inputWeek - 1));
		int dayOfWeek = calFirstDayOfTheYear.get(Calendar.DAY_OF_WEEK);

		Calendar calFirstDayInWeek = (Calendar) calFirstDayOfTheYear.clone();

		calFirstDayInWeek
				.add(Calendar.DATE,

				calFirstDayOfTheYear.getActualMinimum(Calendar.DAY_OF_WEEK)
						- dayOfWeek);

		Date firstDayInWeek = calFirstDayInWeek.getTime();

		long i = firstDayInWeek.getTime();
		i = i + 1000 * 60 * 60 * 24;
		Date date1 = new Date(i);
		SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd");
		String str = sf.format(date1);

		// System.out.println(year + "年第" + inputWeek + "个礼拜的第一天是" + str);

		Calendar calLastDayInWeek = (Calendar) calFirstDayOfTheYear.clone();

		calLastDayInWeek
				.add(Calendar.DATE,

				calFirstDayOfTheYear.getActualMaximum(Calendar.DAY_OF_WEEK)
						- dayOfWeek);

		Date lastDayInWeek = calLastDayInWeek.getTime();

		long j = lastDayInWeek.getTime();
		j = j + 1000 * 60 * 60 * 24;
		Date date2 = new Date(j);
		String str2 = sf.format(date2);

		map.put("beginTime", str);
		map.put("endTime", str2);

		// System.out.println(year + "年第" + inputWeek + "个礼拜的最后一天是" + str2);
		return str + "_" + str2;
	}
	public String formatNumberToHourMinuteSecond(Double dateDou){
		String ft = "00:00:00";//没匹配上时:1.等于0时; 2.大于等于86400时.
		BigDecimal d = new BigDecimal(dateDou).setScale(0, BigDecimal.ROUND_HALF_UP);//四舍五入
		int date = Integer.valueOf(d.toString());
		if(date > 0 && date < 10){
			ft = "00:00:0" + date;
		}else if(date >= 10 && date < 60){
			ft = "00:00:" + date;
		}else if(date >= 60 && date < 3600){
			ft = "00:" + (date/60>=10?date/60:"0"+date/60) + ":" + (date%60>=10?date%60:"0"+date%60);
		}else if(date >= 3600 && date < 86400 ){
			ft = (date/3600>=10?date/3600:"0"+date/3600) + ":" + (date%3600/60>=10?date%3600/60:"0"+date%3600/60) + ":" + (date%60>=10?date%60:"0"+date%60);
		}
		return ft;
	}

	public String getYesterdayTime() {
		return getYesterdayTime("yyyy-MM-dd");
		
	}

}
