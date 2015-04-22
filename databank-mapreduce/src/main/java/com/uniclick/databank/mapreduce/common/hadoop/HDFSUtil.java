package com.uniclick.databank.mapreduce.common.hadoop;

import java.io.IOException;
import java.net.URI;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.uniclick.databank.mapreduce.common.Constants;


public class HDFSUtil {

	/**
	 * get FS Default Name Str
	 * 
	 * @return "hdfs://namenode:8020"
	 */
	public static String getFSDefaultNameStr() {
		return "hdfs://" + Constants.HDFS_SERVER_HOST + ":" + Constants.HDFS_FS_PORT;
	}

	/**
	 * get MR Job Tracker Str
	 * 
	 * @return "namenode:8021"
	 */
	public static String getMRJobTrackerStr() {
		return Constants.HDFS_SERVER_HOST + ":" + Constants.HDFS_MAPRED_PORT;
	}

	/**
	 * judge Dir
	 * 
	 * @param path
	 * @return
	 */
	public static boolean judgeDir(String path) {
		boolean flag = false;
		Configuration conf = new Configuration();
		Path p = new Path(path);
		try {
			FileSystem fs = FileSystem.get(URI.create(path), conf);
			flag = fs.exists(p);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return flag;
	}

	/**
	 * judgeDir
	 * 
	 * @param path
	 * @param conf
	 * @return
	 */
	public static boolean judgeDir(String path, Configuration conf) {
		boolean flag = false;
		Path p = new Path(path);
		try {
			FileSystem fs = FileSystem.get(URI.create(path), conf);
			flag = fs.exists(p);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return flag;
	}

	/**
	 * delete hdfs path
	 * 
	 * @param path
	 */
	public static void deleteHDFSPath(String path, Configuration conf) {
		if (judgeDir(path)) {
			try {
				FileSystem fs = FileSystem.get(URI.create(path), conf);
				fs.delete(new Path(path), true);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * get Date Filter Path
	 * 
	 * @param startDateStr
	 * @param endDateStr
	 * @return
	 * @throws ParseException 
	 */
	public static String getDateFilterPath(String startDateStr, String endDateStr) throws ParseException {
		if (startDateStr != null && "".equals(startDateStr) && endDateStr != null && "".equals(endDateStr)) {
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			Date startDate = df.parse(startDateStr);
			Date endDate = df.parse(endDateStr);
			if (startDate.compareTo(endDate) < 1) {
				StringBuffer returnSb = new StringBuffer("{");
				Calendar startCalendar = Calendar.getInstance();
				startCalendar.setTime(startDate);
				Calendar endCalendar = Calendar.getInstance();
				endCalendar.setTime(endDate);
				while (startCalendar.compareTo(endCalendar) < 0) {
					returnSb.append(df.format(startCalendar.getTime())).append(",");
					startCalendar.add(Calendar.DATE, 1);
				}
				returnSb.append(df.format(startCalendar.getTime())).append("}");
				return returnSb.toString();
			}
		}
		return null;
	}

	/**
	 * @param args
	 * @throws ParseException 
	 */
	public static void main(String[] args) throws ParseException {
		System.out.println(getDateFilterPath("2014-06-03", "2014-06-02"));
	}

}
