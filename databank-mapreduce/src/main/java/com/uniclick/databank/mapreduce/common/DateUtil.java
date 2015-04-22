package com.uniclick.databank.mapreduce.common;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtil {

	public static void main(String[] args) throws ParseException {
		//System.out.println();
		//Date yesterday = DateUtil.getDateBefore(new Date(), 1);
		System.out.println(addDay("2014-07-06", 31));
		SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd");
	}
	
	 
	 public static String dateToWeek(String dateStr){
		 SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		 String prefix;
		 String days;
		 Calendar calendar;
		 try {
			Date date = sdf.parse(dateStr);	
			prefix = "星期";
			days = "日一二三四五六";
			calendar = Calendar.getInstance();
			calendar.setTime(date);
			return prefix+days.charAt(calendar.get(Calendar.DAY_OF_WEEK)-1);
		 } catch (ParseException e) {
			 // TODO Auto-generated catch block
			 e.printStackTrace();
			 return null;
		}	  
	 }
	
	public static String strToDate(String str) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Date date = new Date();
		String startDate="";
		if (str.equals("今天")) {
			startDate=sdf.format(date);
		}
		else if (str.equals("昨天")) {
			startDate=sdf.format(getDateBefore(date, 1));
		} else if (str.equals("最近三天")) {
			startDate=sdf.format(getDateBefore(date, 3));
		} else if (str.equals("最近一周")) {
			startDate=sdf.format(getDateBefore(date, 6));
		} else if (str.equals("最近一月")) {
			startDate=sdf.format(getDateMonthBefore(date, 1));
		}
		return startDate;
	}
	public static String strToDate(String dateStr,String str) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Date date;
		try {
			date = sdf.parse(dateStr);
			String startDate="";
			if (str.equals("今天")) {
				startDate=sdf.format(date);
			}else if (str.equals("昨天")) {
				startDate=sdf.format(getDateBefore(date, 1));
			}else if (str.equals("明天")) {
				startDate=sdf.format(getDateBefore(date, -1));
			} else if (str.equals("最近三天")) {
				startDate=sdf.format(getDateBefore(date, 3));
			} else if (str.equals("最近一周")) {
				startDate=sdf.format(getDateBefore(date, 6));
			} else if (str.equals("最近一月")) {
				startDate=sdf.format(getDateMonthBefore(date, 1));
			}
			return startDate;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		
		
	}

	public static Date getDateBefore(Date d, int day) {
		Calendar now = Calendar.getInstance();
		now.setTime(d);
		now.set(Calendar.DATE, now.get(Calendar.DATE) - day);
		return now.getTime();
	}
	
	public static String addDay(String dateStr,int day){
		Date date = CalendarUtil.str2Date(dateStr);
		Date newDate = getDateBefore(date, 0 - day);
		SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd");
		return sf.format(newDate);
	}

	public static Date getDateMonthBefore(Date d, int month) {
		Calendar now = Calendar.getInstance();
		now.setTime(d);
		now.set(Calendar.MONTH, now.get(Calendar.MONTH) - month);
		return now.getTime();
	}
	
	public static String getPrecedingHour(Date date){
		SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		long j = date.getTime();
		j = j -  1000*60*60;
		Date date1 = new Date(j);
		String strDate = sf.format(date1);	
        return strDate;
	}
	
	public static String getHourBefore(Date date,int someHour) {
		SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		long j = date.getTime();
		j = j -  1000*60*60*someHour;
		Date date1 = new Date(j);
		String strDate = sf.format(date1);	
        return strDate;
	}
	
	public static Date addASecond(Date date) {
		long j = date.getTime();
		j = j - 1500;
		Date date1 = new Date(j);
        return date1;
	}
	
	/**
	 * 得到这个日期属于哪个年度的哪个季度   
	 * @param time   格式  yyyy-MM-dd
	 * @return 格式  年度后面紧跟季度     如2013第一季度  ： 201301 
	 */
	public static String getQuarter(String time) {
		String[] arrTime = time.split("-");
		String year = arrTime[0];
		String month = arrTime[1].trim();
		String quarter = "01";
		if("01".equals(month)|| "02".equals(month)|| "03".equals(month)) {
			quarter = "01";
		}else if("04".equals(month)|| "05".equals(month)|| "06".equals(month)) {
			quarter = "02";
		}else if("07".equals(month)|| "08".equals(month)|| "09".equals(month)) {
			quarter = "03";
		}else{
			quarter = "04";
		}
		return year+quarter;
	}
}
