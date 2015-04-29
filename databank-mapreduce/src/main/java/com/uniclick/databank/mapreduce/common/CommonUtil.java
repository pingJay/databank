package com.uniclick.databank.mapreduce.common;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URLDecoder;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import com.uniclick.databank.mapreduce.job.StandardTarget;


/**
 * 
 * @author yang qi
 * @date 2012-05-15
 * @version 1.0
 * @function 任务公用方法
 */
public class CommonUtil {
	public static final String PV = "展现数";
	public static final String CLK = "点击数";
	public static final String UV = "唯一展现数(cookie)";
	public static final String UC = "唯一点击数(cookie)";
	public static final String VIP = "唯一展现数(IP)";
	public static final String CIP = "唯一点击数(IP)";
	public static final String UIP = "唯一IP数";
	public static final String AVGDURATION = "页面平均停留时间";
	public static final String VISIT = "访问数";
	private static final String OS_ANDROID = "0";
	private static final String OS_IOS = "1";
	private static final String OS_WP = "2";
	private static final String OS_OTHER = "3";
	private static final String IMEI = "imei";
	private static final String MAC = "mac";
	private static final String ANDROID = "android";
	private static final String IDFA = "idfa";
	private static final String OPID = "opid";
	private static final String OS = "os";
	
	private static Configuration conf = new Configuration();
	/* 本地测试专用
	 * static{
		conf.set("fs.default.name", HDFSUtil.getFSDefaultNameStr());
		conf.set("mapred.job.tracker", HDFSUtil.getMRJobTrackerStr());
		conf.setInt("mapred.reduce.tasks", Constants.JOB_CONF_REDUCER_NUMBER);
	}*/
	
	private static SimpleDateFormat  format = new SimpleDateFormat("yyyy-MM-dd");
	
	
	public static boolean isMobile(String uri) {
		if(null != uri && uri.contains("imei=") && uri.contains("mac=") && uri.contains("android=")
				&& uri.contains("idfa=") && uri.contains("opid=") && uri.contains("os=")){
			return true;
		}
		return false;
	}
	
	public static String getMobileDeviceId(String uri,String ip){
		Map<String,String> mobileMap = new HashMap<String,String>();
		uri = decoderURL(uri);
		int startIndex = uri.indexOf(IMEI);
		int osIndex = uri.indexOf(OS);
		if(startIndex > -1 && osIndex > -1) {
			int separatorIndex = uri.indexOf("/", osIndex);
			if(separatorIndex > -1) {
				uri = uri.substring(startIndex, separatorIndex);
			}else{
				uri = uri.substring(startIndex);
			}
		}else{
			return ip;
		}
		String[] arrUri = uri.split("&",-1);
		for(String mobileInfo : arrUri) {
			String[] arrMobileInfo = mobileInfo.split("=",-1);
			if(arrMobileInfo.length == 2) {
				mobileMap.put(arrMobileInfo[0].trim(), "".equals(arrMobileInfo[1].trim()) ? "NULL" : arrMobileInfo[1].trim());
			}
		}
			
		/*
		 * 根据不同的操作系统，取不同的值
		 */
		String os = mobileMap.get(OS);
		if(OS_ANDROID.equals(os)) {
			if(!"NULL".equals(mobileMap.get(IMEI))) {
				return mobileMap.get(IMEI);
			}else if(!"NULL".equals(mobileMap.get(ANDROID))) {
				return mobileMap.get(ANDROID);
			}else if(! "NULL".equals(mobileMap.get(MAC))) {
				return mobileMap.get(MAC);
			}
		}else if(OS_IOS.equals(os)) {
			if(!"NULL".equals(mobileMap.get(IDFA))) {
				return mobileMap.get(IDFA);
			}else if(!"NULL".equals(mobileMap.get(OPID))) {
				return mobileMap.get(OPID);
			}else if(! "NULL".equals(mobileMap.get(MAC))) {
				return mobileMap.get(MAC);
			}
		}else if(OS_WP.equals(os) || OS_OTHER.equals(os)) {
			if (! "NULL".equals(mobileMap.get(MAC))) {
				return mobileMap.get(MAC);
			}
		}
		return ip;
	}
	
	public static String decoderURL(String url) {
		String url_bak = url;
		try {
			
			url = java.net.URLDecoder.decode(url, "utf-8");
		} catch (Exception e1) {
		// TODO Auto-generated catch block
			url = url_bak;
		}
		return url;
	}
	
	/**
	 * 
	 * @return 昨天的日期
	 */
	public static String yesterdayDate(){
		Date date = new Date();
		SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd");
		long nowDate = date.getTime();
		long j = 0;
		j = nowDate - 24*60*60*1000;
		Date yesterDay = new Date(j);
		String strDate = sf.format(yesterDay);
		return strDate;
	}
	
	/**
	 * 
	 * @return 前天的时间
	 */
	public static String dayBeforYesterdayDate(){
		Date date = new Date();
		SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd");
		long nowDate = date.getTime();
		long j = 0;
		j = nowDate - 2*24*60*60*1000;
		Date yesterDay = new Date(j);
		String strDate = sf.format(yesterDay);
		return strDate;
	}
	/**
	 * 
	 * @param date 日期
	 * @param outputDur 任务完成后的输出文件路径
	 * @return
	 */
	public static Path[] impAndOutPutLog(String date , String outputDur){
		Path [] impPath = impLog(date);
		Path [] acyPath = listInPutDir(outputDur);
		Path [] intputPath  = new Path[impPath.length+acyPath.length];
		int i= 0 ;
		for(int x=0 ; x<impPath.length ; x++,i++){
			intputPath[i] = impPath[x];
		}
		for(int x=0 ;x<acyPath.length ; x++,i++){
			intputPath[i] = acyPath[x];
		}
		return intputPath;
	}
	
	/**
	 * 
	 * @param field 字段
	 * @param impValue 展现日志输出值
	 * @param clkValue 点击日志输出值
	 * @return 
	 */
	public static String beSureOutputValue(String field , String impValue , String clkValue){
		String [] impArr = impValue.split("\t");
		String [] clkArr = clkValue.split("\t");
		String [] fieldArr = field.split(",");
		int fieldArrLen = fieldArr.length;
		StringBuffer sf = new StringBuffer();
		for(int i=0 ;i<fieldArrLen ; i++){
			if(PV.equals(fieldArr[i])){
				sf.append(impArr[0]).append("\t");
			}else if(UV.equals(fieldArr[i])){
				sf.append(impArr[1]).append("\t");
			}else if(VIP.equals(fieldArr[i])){
				sf.append(impArr[2]).append("\t");
			}else if(CLK.equals(fieldArr[i])){
				sf.append(clkArr[0]).append("\t");
			}else if(UC.equals(fieldArr[i])){
				sf.append(clkArr[1]).append("\t");
			}else if(CIP.equals(fieldArr[i])){
				sf.append(clkArr[2]).append("\t");
			}
		}
		String outputValue = sf.toString().trim();
		return outputValue;
	}

	/**
	 * 
	 * @param field 字段
	 * @param value 输出值
	 * @return
	 */
	public static String beSureOutputValue(String field , String value ){
		String [] valueArr = value.split("\t");
		String [] fieldArr = field.split(",");
		int fieldArrLen = fieldArr.length;
		StringBuffer sf = new StringBuffer();
		for(int i=0 ;i<fieldArrLen ; i++){
			if(VISIT.equals(fieldArr[i])){
				sf.append(valueArr[0]).append("\t");
			}else if(UV.equals(fieldArr[i])){
				sf.append(valueArr[1]).append("\t");
			}else if(UIP.equals(fieldArr[i])){
				sf.append(valueArr[2]).append("\t");
			}else if(AVGDURATION.equals(fieldArr[i])){
				sf.append(valueArr[3]).append("\t");
			}else if(PV.equals(fieldArr[i])){
				sf.append(valueArr[4]).append("\t");
			}
		}
		return sf.toString().trim();
	}
	
	/**
	 * 计算UV , UIP , AvgDuration , PV 的值 
	 * return Visit+"\t"+UV+"\t"+"UIP"+"\t"+AvgDuration+"\t"+PV
	 */
	public static String calCulateValue(String[] arr){
		int len = arr.length;
		Set<String> set_uv = new HashSet<String>();//用来计算UV
		Set<String> set_uip = new HashSet<String>();//用来计算UIP
		int sum_pv = 0 ;
		String avgDuration = null;
		double  sum_duration = 0;
		for(int i=0 ;i<arr.length ;i++){
			String [] arr_split = arr[i].split("\t");
			set_uv.add(arr_split[0].trim());
			set_uip.add(arr_split[1].trim());
			sum_duration += Double.valueOf(arr_split[2].trim());
			sum_pv += Integer.valueOf(arr_split[3].trim());
		}
		BigDecimal big = new BigDecimal(sum_duration/len); 
		avgDuration = String.valueOf(big.divide(new BigDecimal(1), 2, BigDecimal.ROUND_HALF_UP).doubleValue());
		return len+"\t"+set_uv.size()+"\t"+set_uip.size()+"\t"+avgDuration+"\t"+sum_pv;
	}
	
	/**
	 * 确定JOB分析日志类型
	 * @param metrics 字段值
	 * @param type 
	 * @return
	 */
	public static boolean beSureAnalyLogType(String metrics , String type){
		String [] arr = metrics.split(",");
		int arrLen = arr.length;
		if("imp".equals(type)){
			for(int i=0 ;i<arrLen ; i++){
				if(PV.equals(arr[i])||UV.equals(arr[i])||VIP.equals(arr[i])){
					return true;
				}
			}
		}else if("clk".equals(type)){
			for(int i=0 ;i<arrLen ; i++){
				if(CLK.equals(arr[i])||UC.equals(arr[i])||CIP.equals(arr[i])){
					return true;
				}
			}
		}
		return false;
	}
	
	/**
	 * @param input_time 根据输入的日期得到指定的日志的路径
	 * @return
	 */
	public static Path[] pageCrowdDailyLog(String input_time){
		  Path [] listPath = null;
	      String uri = "hdfs://namenode/user/deepsight/tbl_page_crown/daily/*/*";
	      FileSystem fs;
		try {
			  fs = FileSystem.get(URI.create(uri), conf);
		      FileStatus[] status = fs.globStatus(new Path("hdfs://namenode/user/deepsight/tbl_page_crown/daily/*"+input_time+"*/*"));
	          listPath = FileUtil.stat2Paths(status);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return listPath;
	}
	
	/**
	 * @param date 日期
	 * @return Path[]
	 * @description 获取指定日期的展现日志路径
	 */
	public static Path[] dapImpLog(String date){
		  Path [] listPath = null;
	      String uri = "hdfs://namenode/user/deepsight/cookie_pc/*/*";
	      FileSystem fs;
		try {
			  fs = FileSystem.get(URI.create(uri), conf);
		      FileStatus[] status = fs.globStatus(new Path("hdfs://namenode/user/deepsight/cookie_pc/*"+date+"*/tbl_uni_imp_log*"));
	          listPath = FileUtil.stat2Paths(status);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return listPath;
	}
	
	/**
	 * @param date 日期
	 * @return Path[]
	 * @description 获取指定日期的点击日志路径
	 */
	public static Path[] dapClkLog(String date){
		  Path [] listPath = null;
	      String uri = "hdfs://namenode/user/deepsight/cookie_pc/*/*";
	      FileSystem fs;
		try {
			  fs = FileSystem.get(URI.create(uri), conf);
		      FileStatus[] status = fs.globStatus(new Path("hdfs://namenode/user/deepsight/cookie_pc/*"+date+"*/tbl_uni_clk_log*"));
	          listPath = FileUtil.stat2Paths(status);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return listPath;
	}
	/**
	 * @param path hadoop系统中的路径
	 * @return 如果存在返回true,不存在返回false
	 */
	public static boolean judgeDir(String path){
		boolean flag = false;
		Path p = new Path(path);
		try {
			FileSystem fs = FileSystem.get(URI.create(path), conf);
			flag = fs.exists(p);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return flag;
	}
	
	/**
	 * @description 删除目录
	 * @throws Exception
	 */
	public static void  delete(String outputDir) {
		try {
			FileSystem fs = FileSystem.get(URI.create(outputDir),conf);
			Path path = new Path(outputDir);
			fs.delete(path);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * 
	 * @return 昨天的日期
	 */
	public static String yesterdaytime(){
		Date date = new Date();
		SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd");
		long nowDate = date.getTime();
		long j = 0;
		j = nowDate - 24*60*60*1000;
		Date yesterDay = new Date(j);
		String strDate = sf.format(yesterDay);
		return strDate;
	}
	
	/**
	 * 
	 * @return Path[]
	 * @description  返回该job输出路径下所有输出文件路径
	 */
	public static Path[] listOutPutDir(String outputDir){
		  Path [] listPath = null;
	      String uri = outputDir+"/part-*";
	      FileSystem fs;
		try {
			  fs = FileSystem.get(URI.create(uri), conf);
		      FileStatus[] status = fs.globStatus(new Path(uri));
	          listPath = FileUtil.stat2Paths(status);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return listPath;
	}
	
	
	/**
	 * @param date 日期
	 * @return Path[]
	 * @description 获取指定日期的展现日志路径
	 */
	public static Path[] impLog(String date){
		  Path [] listPath = null;
	      String uri = "hdfs://namenode/user/hive/warehouse/hive_tbl_imp_log_v2/*/*/";
	      FileSystem fs;
		try {
			  fs = FileSystem.get(URI.create(uri), conf);
		      FileStatus[] status = fs.globStatus(new Path("hdfs://namenode/user/hive/warehouse/hive_tbl_imp_log_v2/*"+date+"*/*/"));
	          listPath = FileUtil.stat2Paths(status);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return listPath;
	}
	
	/**
	 * @param date 日期
	 * @return Path[]
	 * @description 获取指定日期的点击日志路径
	 */
	public static Path[] clkLog(String date){
		  Path [] listPath = null;
	      String uri = "hdfs://namenode/user/hive/warehouse/hive_tbl_clk_log_v2/*/*/";
	      FileSystem fs;
		try {
			  fs = FileSystem.get(URI.create(uri), conf);
		      FileStatus[] status = fs.globStatus(new Path("hdfs://namenode/user/hive/warehouse/hive_tbl_clk_log_v2/*"+date+"*/*/"));
	          listPath = FileUtil.stat2Paths(status);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return listPath;
	}
	
	/**
	 * @param date 日期
	 * @return Path[]
	 * @description 获取指定日期的点击日志路径
	 */
	public static Path[] siteLog(String date){
		  Path [] listPath = null;
	      String uri = "hdfs://namenode/user/hive/warehouse/hive_tbl_site_log_v2/*/*/";
	      FileSystem fs;
		try {
			  fs = FileSystem.get(URI.create(uri), conf);
		      FileStatus[] status = fs.globStatus(new Path("hdfs://namenode/user/hive/warehouse/hive_tbl_site_log_v2/*"+date+"*/*/"));
	          listPath = FileUtil.stat2Paths(status);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return listPath;
	}
	
	/**
	 * @param date 日期
	 * @return Path[]
	 * @description 获取指定日期的点击日志路径
	 */
	public static Path[] acyLog(String date){
		  Path [] listPath = null;
	      String uri = "hdfs://namenode/user/hive/warehouse/hive_tbl_acy_log_v2/*/*/";
	      FileSystem fs;
		try {
			  fs = FileSystem.get(URI.create(uri), conf);
		      FileStatus[] status = fs.globStatus(new Path("hdfs://namenode/user/hive/warehouse/hive_tbl_acy_log_v2/*"+date+"*/*/"));
	          listPath = FileUtil.stat2Paths(status);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return listPath;
	}
	
	/**
	 * @param date 日期
	 * @return Path[]
	 * @description 获取指定日期的点击日志路径
	 */
	public static Path[] pageVisitLog(String date){
		  Path [] listPath = null;
	      String uri = "hdfs://namenode/user/deepsight/tbl_page_visit_log/*/*";
	      FileSystem fs;
		try {
			  fs = FileSystem.get(URI.create(uri), conf);
		      FileStatus[] status = fs.globStatus(new Path("hdfs://namenode/user/deepsight/tbl_page_visit_log/*"+date+"*/*"));
	          listPath = FileUtil.stat2Paths(status);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return listPath;
	}

	/**
	 * 
	 * @param formerOutPutDir hadoop系统路径
	 * @return 路径集合
	 */
	public static Path[] listInPutDir(String formerOutPutDir){
		  Path [] listPath = null;
	      FileSystem fs;
		try {
			  fs = FileSystem.get(URI.create(formerOutPutDir), conf);
		      FileStatus[] status = fs.globStatus(new Path(formerOutPutDir+"/part-*"));
	          listPath = FileUtil.stat2Paths(status);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return listPath;
	}
	
	/**
	 * 
	 * @param date1 日期
	 * @param date2 日期
	 */
	public static String [] arrStartAndEndTime(String date1, String date2){
		String [] arr = null;
		if(date1.equals(date2)){//判断两日期是否相等
			arr = new String[1];
			arr[0] = date1;
			return arr;
		}
		
		String tmp;
		if(date1.compareTo(date2) > 0){  //确保 date1的日期不晚于date2
			tmp = date1; date1 = date2; date2 = tmp;
		}
		long x = str2Date(date2).getTime()-str2Date(date1).getTime();
		int len = (int)((str2Date(date2).getTime()-str2Date(date1).getTime())/(3600*24*1000))+1;
		arr = new String[len];
		arr[0] = date1;
		tmp = format.format(str2Date(date1).getTime() + 3600*24*1000);//起始日期+1DAY
		
        int num = 0; 
        while(tmp.compareTo(date2) < 0){        	        
        	num++;
        	arr[num] = tmp;
        	tmp = format.format(str2Date(tmp).getTime() + 3600*24*1000);
        }
        arr[len-1] = date2;
        return arr;
	}
	
	/**
	 * 
	 * @param str 日期
	 * @return
	 * @function String 日期 转换为  Date 日期
	 */
	public static Date str2Date(String str) {
		if (str == null) return null;
		
		try {
			return format.parse(str);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/***
	 * @param input_time 时间
	 * @throws Exception
	 * @description 通过制定分析结果文件的路径时间戳来读取结果文件的内容并将其move到指定linux本地服务器指定的位置
	 */
	public static void ftpToLocal(String input_time,String outputDir ,String localDir , String type)throws Exception{
		if(judgeDir(outputDir)){
		InputStream is = null;
		try {
			FileSystem fs = FileSystem.get(URI.create(outputDir), conf);//
			String strPath = "/home/hadoop/AnalyseCenter/"+localDir+"/"+type+"/";
			File file  = new File(strPath);
			file.mkdirs();
			String local_file_dir = strPath+input_time+localDir+type+".txt";
			File local_file = new File(local_file_dir);
			if(local_file.exists()){
				local_file.delete();
			}
			Path [] part_path = listInPutDir(outputDir);
			for(int i=0 ; i<part_path.length;i++){
			is = fs.open(part_path[i]);
			OutputStream os = new FileOutputStream(local_file_dir,true);
			byte data[] = new byte[1024];
			int temp = 0;
			while((temp=is.read(data))!=-1){
				os.write(data, 0, temp);
			}
			os.close();
			IOUtils.copyBytes(is, System.out, 4096, false);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			IOUtils.closeStream(is);
			e.printStackTrace();
		}
		}
	}

	
	/**
	 * 
	 * @param date1 起始日期
	 * @param date2 结束日期
	 * @return  字符串 {2012-05-01,2012-05-02}
	 */
	public static String strStartAndEndTime(String date1, String date2){
		StringBuffer sf = new StringBuffer();
		sf.append("{"+date1);
		if(date1.equals(date2)){//判断两日期是否相等
			sf.append("}");
			return sf.toString();
		}
		
		String tmp;
		if(date1.compareTo(date2) > 0){  //确保 date1的日期不晚于date2
			tmp = date1; date1 = date2; date2 = tmp;
		}
		
		tmp = format.format(str2Date(date1).getTime() + 3600*24*1000);//起始日期+1DAY
		
        int num = 0; 
        while(tmp.compareTo(date2) < 0){        	        
        	num++;
        	sf.append(","+tmp);
        	tmp = format.format(str2Date(tmp).getTime() + 3600*24*1000);
        }
        sf.append(","+date2+"}");
        return sf.toString();
	}	
	
	
	/**
	 * 
	 * @param date 日期
	 * @return 同时读入展现点击输出文件路径
	 */
	public static Path[] impAndClkLog(String impOutputDir , String clkOutputDir){
		Path [] impPath = listInPutDir(impOutputDir);
		Path [] clkPath = listInPutDir(clkOutputDir);
		Path [] intputPath  = new Path[impPath.length+clkPath.length];
		int i= 0 ;
		for(int x=0 ; x<impPath.length ; x++,i++){
			intputPath[i] = impPath[x];
		}
		for(int x=0 ;x<clkPath.length ; x++,i++){
			intputPath[i] = clkPath[x];
		}
		return intputPath;
	}
	
	 /**
	  * 具体统计分析网站数据的方法  分析 visit 停留时间  跳出数  pv
	  * @param sortMap 按时间拍好序的map
	  */
	public static String anylaseSiteData(TreeMap<String,String> sortMap) {
		StringBuffer returnStr = new StringBuffer();
		int pv = 0; //总PV
		int visitPV = 0;//一次visit中的PV数
		int visit = 0;
		int duration = 0; //停留时间
		int bounced = 0; //跳出数
		int totalVisitDepth = 0 ;//总访问深度
		Set<String> visitUrl = new HashSet<String>(); //一次visit中的去重URL数
		boolean isFrist = true;
		String startTime = "";
		String visitEndTime = ""; //一次visit中的最后结束时间
		for(String createTime : sortMap.keySet()) {
			String[] arrValue = sortMap.get(createTime).trim().split("\t");
			int logpv = Integer.parseInt(arrValue[0]); //得到MAP阶段中判断过的PV值是1还是0
			String url = arrValue[1].trim();
			
			if(isFrist) {
				visit++;
				visitPV+=logpv;
				startTime = createTime;
				visitEndTime = createTime;
				if(!"".equals(url) || url != null) {
					visitUrl.add(url);
				}
				isFrist = false;
			}else{
				//判断这次访问时间与当前这个visit的第一个访问时间是否超过30分钟
				if(betweenThirtyM(visitEndTime,createTime)) {
					visit++; //一个新的visit
					duration += betweentime(startTime,visitEndTime);
					totalVisitDepth += visitUrl.size();
					if(visitPV == 1) {
						bounced++; //如果一次visit中只有一个PV，那就说明只访问的了一个页面就跳出了。跳出数加1
					}
					pv += visitPV;
					
					//一次visit之后对基于一次visit的变量重新初始化
					visitPV = logpv;
					startTime = createTime;
					visitEndTime = createTime;
					visitUrl = new HashSet<String>();
					if(!"".equals(url) || url != null) {
						visitUrl.add(url);
					}
				}else{//还在一次visit中，visitPV加1，visitEndTime更新为当前这个时间
					visitPV+=logpv;
					visitEndTime = createTime;
					if(!"".equals(url) || url != null) {
						visitUrl.add(url);
					}
				}
			}
		}
		
		//全部循环完，如果最后一次visitPV是1,则跳出数要加1
		if(visitPV == 1) {
			bounced++;
		}
		pv+=visitPV;
		duration += betweentime(startTime,visitEndTime);
		totalVisitDepth += visitUrl.size();
		
		returnStr.append(visit).append("\t").append(pv).append("\t").append(bounced).
		append("\t").append(duration)
		.append("\t").append(totalVisitDepth);
		// visit pv bounced duration totalVisitDepth
		return returnStr.toString();
	}
	
	/**
	 * 返回2个时间间隔多少秒
	 * @param startTime
	 * @param endTime
	 * @return
	 */
	public static long betweentime(String startTime,String endTime) {
		long timeGap = 0l;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try {
			Date startDate = sdf.parse(startTime);
			Date endDate = sdf.parse(endTime);
			timeGap = (endDate.getTime() - startDate.getTime())/ 1000; //结束时间 与开始时间的间隔  单位为秒
		}catch (ParseException e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		return timeGap;
	}
	

	/**
	 * 返回2个时间间隔是否超过30分钟
	 * @param startTime
	 * @param endTime
	 * @return 
	 */
	public static boolean  betweenThirtyM(String startTime,String endTime) {
		boolean flag = true;
		long thirtyM = 60*30; //30分钟为多少毫秒
		long timeGap =	betweentime(startTime,endTime); //结束时间 与开始时间的间隔  单位为毫秒
		if(timeGap <= thirtyM) {
			flag = false;
		}
		return flag;
	}
	
	/**
	  * 具体统计分析网站数据的方法  分析 visit 停留时间  跳出数  pv
	  * @param sortMap 按时间拍好序的map
	  */
	public static Map<String,StandardTarget> anylaseSiteDataAndRefer(TreeMap<String,String> sortMap) {
		Map<String,StandardTarget> refer_date = new HashMap<String,StandardTarget>();//将同种来源的信息保存一个
		int visitPV = 0;//一次visit中的PV数
		int duration = 0; //停留时间
		int bounced = 0; //跳出数
		int totalVisitDepth = 0 ;//总访问深度
		//String visitRefer = "";
		String clkCookieAdIds = "NULL";
		String visitUrl = "";
		String referInfo ;
		String lastReferInfo = "";
		Set<String> visitUrlSet = new HashSet<String>(); //一次visit中的去重URL数
		List<String> visitUrlList = new ArrayList<String>(); //一次visit中的全部url集合
		List<String> visitReferList = new ArrayList<String>(); //一次visit中的全部refer集合
		boolean isFrist = true;
		String startTime = "";
		String visitEndTime = ""; //一次visit中的最后结束时间
		StandardTarget st ;
		for(String createTime : sortMap.keySet()) {
			String[] arrValue = sortMap.get(createTime).split("\t");
			int logpv = Integer.parseInt(arrValue[2]); //得到MAP阶段中判断过的PV值是1还是0
			String url = arrValue[3].trim().replaceAll("%3A", ":").replaceAll("%2F", "/");
			String refer = arrValue[0].trim().replaceAll("%3A", ":").replaceAll("%2F", "/");
			if(isFrist) {
				visitPV+=logpv;
				startTime = createTime;
				visitEndTime = createTime;
				if(!"".equals(url) && url != null) {
					visitUrlSet.add(url);
					visitUrlList.add(url);
					visitReferList.add(refer);
				}
				//visitRefer = arrValue[0].trim();
				clkCookieAdIds = arrValue[1].trim();
				visitUrl = url;
				isFrist = false;
			}else{
				//判断这次访问时间与当前这个visit的第一个访问时间是否超过30分钟
				if(betweenThirtyM(visitEndTime,createTime)) {
					//visit++; //一个新的visit
					duration = (int) betweentime(startTime,visitEndTime);
					totalVisitDepth = visitUrlSet.size();
					if(visitPV == 1) {
						bounced = 1; //如果一次visit中只有一个PV，那就说明只访问的了一个页面就跳出了。跳出数加1
					}
					referInfo = getReferInfo(visitReferList, clkCookieAdIds, visitUrlList, lastReferInfo);
					lastReferInfo = referInfo;
					if(refer_date.containsKey(referInfo)) {
						st = refer_date.get(referInfo);
						st.setPv(st.getPv() + visitPV);
						st.setDarution(st.getDarution() + duration);
						st.setVisit(st.getVisit() + 1);
						st.setBounce(st.getBounce() + bounced);
						st.setUniVisitDepth(st.getUniVisitDepth() + totalVisitDepth);
					}else {
						st = new StandardTarget();
						st.setPv(visitPV);
						st.setDarution(duration);
						st.setVisit(1);
						st.setBounce(bounced);
						st.setUniVisitDepth(totalVisitDepth);
					}
					refer_date.put(referInfo, st);
					
					//一次visit之后对基于一次visit的变量重新初始化
					visitPV = logpv;
					startTime = createTime;
					visitEndTime = createTime;
					visitUrlSet = new HashSet<String>();
					visitUrlList = new ArrayList<String>();
					visitReferList = new ArrayList<String>();
					if(!"".equals(url) && url != null) {
						visitUrlSet.add(url);
						visitUrlList.add(url);
						visitReferList.add(refer);
					}
					bounced = 0;
					duration = 0;
					totalVisitDepth = 0;
					//visitRefer = arrValue[0].trim();
					clkCookieAdIds = arrValue[1].trim();
					visitUrl = url;
				
				}else{//还在一次visit中，visitPV加1，visitEndTime更新为当前这个时间
					visitPV+=logpv;
					visitEndTime = createTime;
					clkCookieAdIds = arrValue[1].trim();
					if(!"".equals(url) && url != null) {
						visitUrlSet.add(url);
						visitUrlList.add(url);
						visitReferList.add(refer);
					}
				}
			}
		}
		
		//全部循环完，如果最后一次visitPV是1,则跳出数要加1
		if(visitPV == 1) {
			bounced = 1 ;
		}
		duration = (int) betweentime(startTime,visitEndTime);
		totalVisitDepth = visitUrlSet.size();
		referInfo = getReferInfo(visitReferList, clkCookieAdIds, visitUrlList, lastReferInfo);
		lastReferInfo = referInfo;
		if(refer_date.containsKey(referInfo)) {
			st = refer_date.get(referInfo);
			st.setPv(st.getPv() + visitPV);
			st.setDarution(st.getDarution() + duration);
			st.setVisit(st.getVisit() + 1);
			st.setBounce(st.getBounce() + bounced);
			st.setUniVisitDepth(st.getUniVisitDepth() + totalVisitDepth);
		}else {
			st = new StandardTarget();
			st.setPv(visitPV);
			st.setDarution(duration);
			st.setVisit(1);
			st.setBounce(bounced);
			st.setUniVisitDepth(totalVisitDepth);
		}
		refer_date.put(referInfo, st);
		return refer_date;
	}
	
	/**
	 * 得到来源的类型和sourceParam（针对于来源为投放的该字段为广告数据,推荐为网站,搜索为搜索引擎,直接来源为NULL）
	 * @param refer
	 * @param clkCookieAdIds
	 * @return
	 */
	public static String getReferInfo(List<String> referList,String clkCookieAdIds,List<String> urlList,String lastReferInfo) {
		String outReferInfo = null; //外部来源信息
		String adReferInfo = null;//广告投放来源信息
		String searchReferInfo = null;//搜索来源信息
		String recomReferInfo = null; //推荐来源信息
		String directReferInfo = null;//直接来源信息
		int referIndex = 0;
		String refer = "";
		boolean isFrist = true; //标记第一个访问来源，因为如果没有外部，投放，搜索这3中来源时，那么第一条是什么就是什么
		String fristRefer = null; //记录第一条的来源信息
		for(String url : urlList) {
			int SourceType = 3;//默认为直接来源
			String orderId = "0";
			String mediaId = "0";
			String sourceParam = "0";
			boolean isrecomSelf = false; //判断是否为本站推荐
			refer = referList.get(referIndex++);
			if(url.contains("uni_campaign") || url.contains("uni_source") || url.contains("uni_channel") || 
					url.contains("uni_content") || url.contains("uni_term")) {//外部渠道
				SourceType = 4;
				String paraValue = null; //用来占时保存URL每个参数的值
				StringBuffer sb = new StringBuffer();
				String url_bak = url; //为防止URL格式化出错
				try {
					url = java.net.URLDecoder.decode(url,"utf-8");
				} catch (Exception e) {
					url = url_bak;
				}
				paraValue=subUrlPara(url,"uni_campaign");
				if(paraValue != null && !"".equals(paraValue.trim())) {
					sb.append("uni_campaign=").append(paraValue).append("&");
				}
				paraValue=subUrlPara(url,"uni_source");
				if(paraValue != null && !"".equals(paraValue.trim())) {
					sb.append("uni_source=").append(paraValue).append("&");
				}
				paraValue=subUrlPara(url,"uni_channel");
				if(paraValue != null && !"".equals(paraValue.trim())) {
					sb.append("uni_channel=").append(paraValue).append("&");
				}
				paraValue=subUrlPara(url,"uni_content");
				if(paraValue != null && !"".equals(paraValue.trim())) {
					sb.append("uni_content=").append(paraValue).append("&");
				}
				paraValue=subUrlPara(url,"uni_term");
				if(paraValue != null && !"".equals(paraValue.trim())) {
					sb.append("uni_term=").append(paraValue).append("&");
				}
				sourceParam = sb.toString();
			}else if(!"".equals(clkCookieAdIds) && clkCookieAdIds != null && !"NULL".equals(clkCookieAdIds)) {
				SourceType = 0; //投放来源
				String[] arrCcaids = clkCookieAdIds.split("\\:");
				String orderItemId = arrCcaids[5].trim();
				sourceParam = orderItemId;
				orderId = arrCcaids[2].trim();
				mediaId = arrCcaids[0].trim();
				/**
				 * siteid在系统中18是百度，129为谷歌 
				 * 我们将这种情况的投放流量算作 付费搜索引擎
				 */
				if("18".equals(mediaId) || "129".equals(mediaId)) {
					SourceType = 1;//搜索引擎
				}
			}else{
				if("".equals(refer.trim()) || refer == null || "NULL".equalsIgnoreCase(refer)) {
					
				}else if(isSearch(refer)) {
					SourceType = 1; //搜索来源
					sourceParam = CommonUtil.getDomain(refer);//主域名
					if(!isSearch(sourceParam) || sourceParam.contains("cpro.baidu.com")) {
						SourceType = 2;
					}else{
						orderId = CommonUtil.beSurekeyWord(refer); //sourceParam2关键词
						if("".equals(orderId) || "NULL".equals(orderId)) {
							SourceType = 2;
							orderId="0";
						}
					}
				}else{
					String referdomain = CommonUtil.getDomain(refer);//主域名	
					String urlDomain = CommonUtil.getDomain(url);//URL主域名	
					if(referdomain.equals(urlDomain) && !"".equals(lastReferInfo)) {//如果refer的主域名与URL的主域名相同
						SourceType = 2; //推荐来源
						sourceParam = referdomain;
						isrecomSelf = true;
					}else{
						SourceType = 2; //推荐来源
						sourceParam = referdomain;
					}
				}
			}
			StringBuffer referInfo = new StringBuffer();
			referInfo.append(SourceType).append("\t")
			.append(sourceParam).append("\t")
			.append(orderId).append("\t")
			.append(mediaId);
			if(isFrist) {
				isFrist = false;
				if(isrecomSelf || SourceType == 3) {
					if(!"".equals(lastReferInfo) && lastReferInfo !=null) {
						fristRefer = lastReferInfo;
					}else {
						fristRefer = referInfo.toString();
					}
				}else{
					fristRefer = referInfo.toString();
				}
			}
			if(SourceType == 0) {
				adReferInfo = referInfo.toString();
			}else if(SourceType == 1) {
				searchReferInfo = referInfo.toString();
			}else if(SourceType == 2) {
				if(isrecomSelf) {
					recomReferInfo = lastReferInfo;
				}else{
					recomReferInfo = referInfo.toString();
				}
			}else if(SourceType == 4) {
				outReferInfo = referInfo.toString();
			}else if(SourceType == 3) {
				if(!"".equals(lastReferInfo) && lastReferInfo !=null) {
					directReferInfo = lastReferInfo;
				}else{
					directReferInfo = referInfo.toString();
				}
			}
		}
		
		if(outReferInfo != null) {
			return outReferInfo;
		}else if(adReferInfo != null) {
			return adReferInfo;
		}else if(searchReferInfo != null) {
			return searchReferInfo;
		}else{
			return fristRefer;
		}
		
	}
	
	/**
	 * 截取URL中指定参数项的值
	 * @param url
	 * @param para
	 */
	public static String subUrlPara(String url,String para) {
		int index = url.indexOf(para+"=");
		if(index != -1) {
			int nextConn = url.indexOf("&", index); //下一个连接符&的位置
			if(nextConn != -1) {
				return url.substring(index+ para.length()+1, nextConn);
			}else{
				return url.substring(index+ para.length()+1);
			}
		}else{
			return null;
		}
	}
	
	public static boolean isSearch(String refer) {
		if(refer.contains(".baidu.")||refer.contains(".google.")
						||refer.contains(".bing.")|| refer.contains(".yahoo.")
						|| refer.contains(".so.") || refer.contains("so.360.cn") || refer.contains(".soso.")
						|| refer.contains(".sogou.") ||  refer.contains(".youdao.") || 
						refer.contains(".woso.")) {
			return true;
		}else{
			return false;
		}
	}
	
	public static String getDomain(String http_referer) {
		String domain = http_referer;
		if(http_referer.startsWith("http://")) {
			int next_suq = http_referer.indexOf("/", 7);
			if(next_suq != -1) {
				domain = http_referer.substring(7, next_suq);
			}else{
				domain = http_referer.substring(7, http_referer.length());
			}
		}else if(http_referer.startsWith("https://")){
			int next_suq = http_referer.indexOf("/", 8);
			if(next_suq != -1) {
				domain = http_referer.substring(8, next_suq);
			}else{
				domain = http_referer.substring(8, http_referer.length());
			}
		}else {
			int next_suq = http_referer.indexOf("/");
			if(next_suq != -1) {
				domain = http_referer.substring(0,next_suq);
			}else{
				domain = http_referer.substring(0, http_referer.length());
			}
		}
		
		return domain;
	}
	
	/**
	 * 将传入的网址 如果是搜索页面来的	则将URL中转码过的关键词转换中文显示
	 * @param url
	 * @return
	 */
	public static String beSurekeyWord(String url){
		try {
				url = java.net.URLDecoder.decode(url, "utf-8");
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			return "NULL";
		}
		String returnStr = "NULL";
		String domain = "";
		if(url.contains(".baidu.")){
			domain = "baidu";
			String wdStr = "";
			int indexWd = url.indexOf("wd=");
			int indexWord = url.indexOf("word=");
			int indexq1 = url.indexOf("q1=");
			int indexW = url.indexOf("w=");
			if(indexWord != -1) {
				int indexWdNext = url.indexOf("&",indexWord); //定位 wd= 下一个&的位置
				if(indexWdNext != -1) {
					wdStr = url.substring(indexWord+5, indexWdNext);
				}else{
					wdStr = url.substring(indexWord+5, url.length());
				}
			}else if(indexWd != -1 || indexq1 != -1){
				int indexWdNext = url.indexOf("&",indexWd); //定位 wd= 下一个&的位置
				if(indexWdNext != -1) {
					wdStr = url.substring(indexWd+3, indexWdNext);
				}else{
					wdStr = url.substring(indexWd+3, url.length());
				}
			}else if(indexW != -1) {
				int indexWdNext = url.indexOf("&",indexW); //定位 wd= 下一个&的位置
				if(indexWdNext != -1) {
					wdStr = url.substring(indexW+2, indexWdNext);
				}else{
					wdStr = url.substring(indexW+2, url.length());
				}
			}
			try {
				String wdName = URLDecoder.decode( wdStr,"utf-8");
				if(!isChinese(wdName)) {
					wdName = URLDecoder.decode( wdStr,"gb2312");
				}
				returnStr = wdName;
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				return "NULL";
			}catch (IllegalArgumentException e) {
				returnStr = wdStr;
			}
			
		}else if(url.contains(".sogou.")) {
			domain = "sogou";
			String wdStr = "";
			int indexQuery = url.indexOf("query=");
			int indexSearch = url.indexOf("search=");
			if(indexQuery != -1) {
				int indexWdNext = url.indexOf("&",indexQuery); //定位 query= 下一个&的位置
				if(indexWdNext != -1) {
					wdStr =	url.substring(indexQuery+6, indexWdNext);
				}else{
					wdStr = url.substring(indexQuery+6, url.length());
				}
			}else if(indexSearch != -1) {
				int indexWdNext = url.indexOf("&",indexSearch); //定位 query= 下一个&的位置
				if(indexWdNext != -1) {
					wdStr =	url.substring(indexSearch+7, indexWdNext);
				}else{
					wdStr = url.substring(indexSearch+7, url.length());
				}
			}
			try {
				String wdName = URLDecoder.decode( wdStr,"utf-8");
				if(!isChinese(wdName)) {
					wdName = URLDecoder.decode( wdStr,"gb2312");
				}
				returnStr = wdName;
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				return "NULL";
			}catch (IllegalArgumentException e) {
				returnStr = wdStr;
			}
		}else if(url.contains(".woso.")){
			domain = "woso";
			String wdStr = "";
			int indexWd = url.indexOf("wd=");
			if(indexWd != -1) {
				int indexWdNext = url.indexOf("&",indexWd); //定位 wd= 下一个&的位置
				if(indexWdNext != -1) {
					wdStr = url.substring(indexWd+3, indexWdNext);
				}else{
					wdStr = url.substring(indexWd+3, url.length());
				}
			}
			try {
				String wdName = URLDecoder.decode( wdStr,"utf-8");
				if(!isChinese(wdName)) {
					wdName = URLDecoder.decode( wdStr,"gb2312");
				}
				returnStr = wdName;
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				return "NULL";
			}catch (IllegalArgumentException e) {
				returnStr = wdStr;
			}
		}else{
			int indexWd = 0;
			String wdStr = "";
			if(url.contains(".google.")){
				indexWd = url.indexOf("q=");
				domain = "google";
			}else if(url.contains(".bing.")){
				indexWd = url.indexOf("q=");
				domain = "bing";
			}else if(url.contains(".yahoo.")) {
				indexWd = url.indexOf("p=");
				domain = "yahoo";
			}else if(url.contains(".so.")) {
				indexWd = url.indexOf("q=");
				domain = "360搜索";
			}else if(url.contains("so.360.cn")) {
				indexWd = url.indexOf("q=");
				domain = "360搜索";
			}else if(url.contains(".soso.")) {
				indexWd = url.indexOf("w=");
				domain = "soso";
			}else if(url.contains(".youdao.")) {
				indexWd = url.indexOf("q=");
				domain = "youdao";
			}
			if(indexWd != -1) {
				int indexWdNext = url.indexOf("&",indexWd); //定位 q= 下一个&的位置
				if(indexWdNext != -1) {
					wdStr =	url.substring(indexWd+2, indexWdNext);
				}else{
					wdStr = url.substring(indexWd+2, url.length());
				}
			}
			try {
				String wdName = URLDecoder.decode( wdStr,"utf-8");
				if(!isChinese(wdName)) {
					wdName = URLDecoder.decode( wdStr,"gb2312");
				}
				returnStr = wdName;
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
				return "NULL";
			}catch (IllegalArgumentException e) {
				returnStr = wdStr;
			}
		}
		return replaceSpecialStr(returnStr);
	}
	
	public static  boolean isChinese(String strName) {  
        char[] ch = strName.toCharArray();  
        for (int i = 0; i < ch.length; i++) {  
            char c = ch[i];  
            if (isChinese(c)) {  
                return true;  
            }  
        }  
        return false;  
    } 
	
	public static  boolean isChinese(char c) {  
        Character.UnicodeBlock ub = Character.UnicodeBlock.of(c);  
        if (ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS  
                || ub == Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS  
                || ub == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A  
                || ub == Character.UnicodeBlock.GENERAL_PUNCTUATION  
                || ub == Character.UnicodeBlock.CJK_SYMBOLS_AND_PUNCTUATION  
                || ub == Character.UnicodeBlock.HALFWIDTH_AND_FULLWIDTH_FORMS) {  
            return true;  
        }  
        return false;  
    } 
	
	public static String replaceSpecialStr(String str) {
		str = str.replaceAll("\r\n", "").replaceAll("\n", "").replaceAll("\t", "");
		return str;
	}
	
	
	
	public static void main(String[] args) {
		double result=30.18;
        int min = (int) (result / 60);
        int seceond = (int) (result % 60);
        System.out.println(min);
        System.out.println(seceond);
	}
}
