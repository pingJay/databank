package com.uniclick.databank.mapreduce.job;



import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import com.uniclick.databank.mapreduce.common.CommonUtil;
import com.uniclick.databank.mapreduce.common.DateUtil;
import com.uniclick.databank.mapreduce.redis.RedisUtil;
import com.uniclick.databank.mapreduce.util.DBJDBCUtil;

/**
 * @author yang qi
 * @date 2012-06-06
 * @version 1.0
 * @function MapReduce 阶段
 */
public class MapReduce {

	/**
	 * @function 分析活动地域数据 MAP 阶段
	 * @output  key:厂商 , 活动  , 省份 , ip/cookie
	 *         value: 1
	 *
	 */
	public static class AnalyOrderAreaMap extends Mapper<LongWritable, Text, Text, IntWritable>{
		private Text kt = new Text();
		private IntWritable vt = new IntWritable(1);
		
		private String [] orderIDArr ;
		
		/***
		 * 获取从JobControl端传递过来的日期和活动参数
		 */
		protected void setup(Context context) throws IOException,
		InterruptedException {
			super.setup(context);
			orderIDArr = context.getConfiguration().get("orderID").split(",");
		}
		
		public void map (LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
			String [] rowArr  = value.toString().split("\t");
			int rowLen = rowArr.length;
			if(rowLen>11){
				boolean status = false;
				int len = orderIDArr.length;
				for(int i=0 ;i<len ;i++){
					if(orderIDArr[i].equals(rowArr[8])){
						status = true;
					}
				}
				String regex = "^[0-9]+$";
				Matcher a = Pattern.compile(regex).matcher(rowArr[7]);
				if(a.matches()&&status){
					String advid  = "".equals(rowArr[7])?"0":rowArr[7];
					String orderid = "".equals(rowArr[8])?"0":rowArr[8];
					String ip = "".equals(rowArr[6])?"NULL":rowArr[6];
					String cookie = "".equals(rowArr[5])?"NULL":rowArr[5];
					String region = "".equals(rowArr[11])?"NULL":rowArr[11];
					String keyPre = advid+"\t"+ orderid + "\t"+ region;
					if(rowLen > 22 && CommonUtil.isMobile(rowArr[17])){//imp
						kt.set(keyPre+"\t"+CommonUtil.getMobileDeviceId(rowArr[17],ip)+"\t"+"mobile");
					}else if(rowLen > 14 && CommonUtil.isMobile(rowArr[14])){
						kt.set(keyPre+"\t"+CommonUtil.getMobileDeviceId(rowArr[14],ip)+"\t"+"mobile");
					}else{
						kt.set(keyPre+"\t"+cookie+"\t"+"cookie");
					}
					//厂商 , 活动 , 省份 ,  cookie
					context.write(kt, vt);
					kt.set(keyPre +"\t"+ ip +"\t"+"ip");
					//厂商 , 活动 , 省份 , ip
					context.write(kt, vt);
				}
			}
		}
	}
	/**
	 * @function 分析活动地域数据 REDUCE 阶段
	 * @output  key:厂商 , 活动  , 省份 , ip/cookie
	 *         value: SUM
	 *
	 */
	public static class AnalyOrderAreaReduce  extends Reducer<Text, IntWritable ,Text , IntWritable>{
		
		private Text kt = new Text();
		private IntWritable vt = new IntWritable();
		
		public void reduce (Text key , Iterable<IntWritable> values ,Context context)throws IOException , InterruptedException{
			int sum = 0;
			for(IntWritable value : values){
				sum+=value.get();
			}
			String [] arr = key.toString().split("\t");
			kt.set(arr[0]+"\t"+arr[1]+"\t"+arr[2]+"\t"+arr[4]);
			vt.set(sum);
			context.write(kt, vt);
		}
	}
	
	/**
	 * @function 计算活动地域数据 MAP 阶段
	 * @output  key:厂商 , 活动 , 省份 
	 *         value: sum ip/cookie
	 *
	 */
	public static class SumOrderAreaMap extends Mapper<LongWritable, Text, Text, Text>{
		private Text kt = new Text();
		private Text vt = new Text();
		
		public void map (LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
			String [] arr = value.toString().split("\t");
			kt.set(arr[0]+"\t"+arr[1]+"\t"+arr[2]);
			vt.set(arr[3]+"\t"+arr[4]);
			context.write(kt, vt);
		}
	}
	/**
	 * @function 计算活动地域数据 MAP 阶段
	 * @output  key:厂商 , 活动 ,  省份  
	 *         value: pv , uv ,uip , imp/clk
	 *
	 */
	public static class SumOrderAreaReduce extends Reducer<Text, Text ,Text , Text>{
		private Text vt = new Text();
		private String impOrClk ;
		
		/***
		 * 获取从JobControl端传递过来的日期和活动参数
		 */
		protected void setup(Context context) throws IOException,
		InterruptedException {
			super.setup(context);
			impOrClk  = context.getConfiguration().get("impOrClk");
		}
		public void reduce (Text key , Iterable<Text> values ,Context context)throws IOException , InterruptedException{
			int uv = 0;
			int vip = 0;
			int pv = 0;
			for(Text value : values){
				String [] arr  = String.valueOf(value).split("\t");
				if("ip".equals(arr[0])){
					vip++;
					pv += Integer.valueOf(arr[1]);
				}else if("cookie".equals(arr[0])){
					uv++;
				}
			}
			vt.set(pv+"\t"+uv+"\t"+vip+"\t"+impOrClk);
			context.write(key, vt);
		}
	}
	
	
	/**
	 * @function 合并活动地域的展现点击结果文件
	 * @output  key:厂商 , 活动 , 城市 , 省份 , 国家 , 
	 *         value: pv , uv ,uip , imp/clk
	 *
	 */
	public static class CombineOrderAreaMap extends Mapper<LongWritable, Text, Text, Text>{
		private Text kt = new Text();
		private Text vt = new Text();
		
		public void map (LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
			String [] arr = value.toString().split("\t");
			kt.set(arr[0]+"\t"+arr[1]+"\t"+arr[2]);
			vt.set(arr[3]+"\t"+arr[4]+"\t"+arr[5]+"\t"+arr[6]);
			context.write(kt, vt);
		}
	}
	/**
	 * @function 合并活动地域的展现点击结果文件
	 * @output  key:厂商 , 活动 , 城市 , 省份 , 国家 , 
	 *         value: pv , uv ,uip , clk , uc ,cip
	 *
	 */
	public static class CombineOrderAreaReduce  extends Reducer<Text, Text ,Text , Text>{
		private Text vt = new Text();
		private String metrics = null;
		protected void setup(Context context) throws IOException,
		InterruptedException {
			super.setup(context);
			metrics  = context.getConfiguration().get("metrics");
		}
		
		public void reduce (Text key , Iterable<Text> values ,Context context)throws IOException , InterruptedException{
			String clkValue =0+"\t"+0+"\t"+0;
			String impValue =0+"\t"+0+"\t"+0;
			for(Text value : values){
				String arr[] = value.toString().split("\t");
				if("imp".equals(arr[3])){
					impValue = arr[0]+"\t"+arr[1]+"\t"+arr[2];
				}else{
					clkValue = arr[0]+"\t"+arr[1]+"\t"+arr[2];
				}
			}
			String outputValue = CommonUtil.beSureOutputValue(metrics, impValue, clkValue);
			vt.set(outputValue);
			context.write(key, vt);
		}
	}
	
	
	/** 
	 * @function 分析厂商数据 Map 阶段
	 * @output key : companyid , cookie/ip
	 *       value : 1
	 *
	 */
	public static class AnalyCompanyMap extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		private Text kt = new Text();
		private IntWritable vt = new IntWritable(1);
		
		private String [] companyIDArr ;
		/***
		 * 获取从JobControl端传递过来的日期和活动参数
		 */
		protected void setup(Context context) throws IOException,
		InterruptedException {
			super.setup(context);
			companyIDArr = context.getConfiguration().get("companyID").split(",");
		}
		
		public void map (LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
			String [] arr = value.toString().split("\t");
			int arrLen = arr.length;
				if(arrLen>7){
						int len = companyIDArr.length;
						boolean status = false;
						String advid = arr[7];//厂商ID
						for(int i=0 ;i<len ;i++){
							if(companyIDArr[i].equals( advid)){
								status = true;
								break;
							}
						}
						if(status){
							kt.set(advid+"\t"+arr[6]+"\t"+"ip");//advid+ip
							context.write(kt, vt);
							kt.set(advid+"\t"+arr[5]+"\t"+"cookie");//advid+cookie
							context.write(kt, vt);
						}
				}
		}
	}
	
	/** 
	 * @function 分析厂商数据 Reduce 阶段
	 * @output key : companyid , cookie/ip
	 *       value :sum
	 *
	 */
	public static class AnalyCompanyReduce  extends Reducer<Text, IntWritable ,Text , IntWritable>{
        private Text kt = new Text();
        private IntWritable vt = new IntWritable();
        public void reduce (Text key , Iterable<IntWritable> values ,Context context)throws IOException , InterruptedException{
        	int sum = 0;
			for(IntWritable value : values){
				sum+=value.get();
			}
			String [] arr = key.toString().split("\t");
			kt.set(arr[0]+"\t"+arr[2]);
			vt.set(sum);
			context.write(kt, vt);
        }
	}
	
	/** 
	 * @function 计算厂商数据 Map 阶段
	 * @output key : companyid ,
	 *       value :  cookie/ip sum
	 *
	 */
	public static class SumCompanyMap extends Mapper<LongWritable, Text, Text, Text>{
		
		private Text kt = new Text();
		private Text vt = new Text();
		
		public void map (LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
			String arr[] = value.toString().split("\t");
			kt.set(arr[0]);
			vt.set(arr[1]+"\t"+arr[2]);
			context.write(kt, vt);
		}
	}
	
	
	/**
	 * @function 合并活动地域的展现点击结果文件
	 * @output  key:厂商  
	 *         value: pv , uv ,uip , imp/clk
	 *
	 */
	public static class CombineCompanyMap extends Mapper<LongWritable, Text, Text, Text>{
		private Text kt = new Text();
		private Text vt = new Text();
		
		public void map (LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
			String [] arr = value.toString().split("\t");
			kt.set(arr[0]);
			vt.set(arr[1]+"\t"+arr[2]+"\t"+arr[3]+"\t"+arr[4]);
			context.write(kt, vt);
		}
	}
	
	
	/** 
	 * @function 分析广告位数据   MAP阶段
	 * @output  key: 广告位 , 媒体 ,  厂商 , 活动 , cookie/ip
	 *         value: 1
	 *
	 */
	public static class AnalyInventoryMap extends Mapper<LongWritable, Text, Text, IntWritable>{
		private Text kt = new Text();
		private IntWritable vt = new IntWritable(1);
		private String [] orderIDArr ;
		
		/***
		 * 获取从JobControl端传递过来的日期和活动参数
		 */
		protected void setup(Context context) throws IOException,
		InterruptedException {
			super.setup(context);
			orderIDArr = context.getConfiguration().get("orderID").split(",");
		}
		
		public void map (LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
			String line = value.toString();
			String[] arr = line.split("\t");
			int arrLen = arr.length;
			if(arrLen>8){
				String inventory = arr[1];
				String siteid = arr[3];
				String advid = arr[7];
				String orderid = arr[8];
				String regex = "^[0-9]+$";
				Matcher a = Pattern.compile(regex).matcher(inventory);
				Matcher b = Pattern.compile(regex).matcher(siteid);
				Matcher c = Pattern.compile(regex).matcher(advid); 
				boolean status = false;
				int len = orderIDArr.length;
				for(int i=0 ;i<len ;i++){
					if(orderIDArr[i].equals(orderid)){
						status= true;
						break;
					}
				}
				if(a.matches()&&b.matches()&&c.matches()&&status){
					kt.set(advid+"\t"+orderid+"\t"+siteid+"\t"+inventory+"\t"+arr[6]+"\t"+"ip");
					context.write(kt, vt);
					kt.set(advid+"\t"+orderid+"\t"+siteid+"\t"+inventory+"\t"+arr[5]+"\t"+"cookie");
					context.write(kt, vt);
				}
			}
		}
	}
	
	/** 
	 * @function 分析广告位数据    Reduce阶段
	 * @output  key: 广告位 , 媒体 ,  厂商 , 活动 , cookie/ip
	 *         value: sum
	 *
	 */
	public static class AnalyInventoryReduce  extends Reducer<Text, IntWritable ,Text , IntWritable>{
        private Text kt = new Text();
        private IntWritable vt = new IntWritable();
        public void reduce (Text key , Iterable<IntWritable> values ,Context context)throws IOException , InterruptedException{
        	int sum = 0;
			for(IntWritable value : values){
				sum+=value.get();
			}
			String [] arr = key.toString().split("\t");
			kt.set(arr[0]+"\t"+arr[1]+"\t"+arr[2]+"\t"+arr[3]+"\t"+arr[5]);
			vt.set(sum);
			context.write(kt, vt);
        }
	}
	
	/** 
	 * @function 计算广告位数据 Map 阶段
	 * @output key : 广告位 , 媒体 , 厂商 , 活动 
	 *       value :  cookie/ip sum
	 *
	 */
	public static class SumInventoryMap extends Mapper<LongWritable, Text, Text, Text>{
		
		private Text kt = new Text();
		private Text vt = new Text();
		
		public void map (LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
			String arr[] = value.toString().split("\t");
			kt.set(arr[0]+"\t"+arr[1]+"\t"+arr[2]+"\t"+arr[3]);
			vt.set(arr[4]+"\t"+arr[5]);
			context.write(kt, vt);
		}
	}
	
	/** 
	 * @function 计算广告位数据 Map 阶段
	 * @output key :厂商 , 活动 , 媒体   ,  广告位 
	 *       value : pv , uv , uip , imp/clk
	 *
	 */
	public static class CombineInventoryMap extends Mapper<LongWritable, Text, Text, Text>{
		private Text kt = new Text();
		private Text vt = new Text();
		
		public void map (LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
			String [] arr = value.toString().split("\t");
			//kt.set(arr[2]+"\t"+arr[3]+"\t"+arr[1]+"\t"+arr[0]);
			kt.set(arr[0]+"\t"+arr[1]+"\t"+arr[2]+"\t"+arr[3]);
			vt.set(arr[4]+"\t"+arr[5]+"\t"+arr[6]+"\t"+arr[7]);
			context.write(kt, vt);
		}
	}
	
	
	/** 
	 * @function 分析媒体数据 Map 阶段
	 * @output key : 厂商ID , 活动  , 媒体 , cookie/ip
	 *       value : 1
	 *
	 */
	public static class AnalyMediaMap extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		private Text kt = new Text();
		private IntWritable vt = new IntWritable(1);
		private String [] orderIDArr ;
		
		protected void setup(Context context) throws IOException,
		InterruptedException {
			super.setup(context);
			orderIDArr = context.getConfiguration().get("orderID").split(",");
		}

		
		public void map (LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
			String line = value.toString();
			String [] arr = line.split("\t");
			int arrLen =  arr.length;
			if(arrLen>8){
				int len = orderIDArr.length;
				boolean status = false;
				for(int i=0 ; i<len ;i++){
					if(orderIDArr[i].equals(arr[8])){
						status = true;
						break;
					}
				}
				String regex = "^[0-9]*$";
				String siteid = arr[3];
				String advid = arr[7];
				Matcher b = Pattern.compile(regex).matcher(siteid);
				Matcher c = Pattern.compile(regex).matcher(advid);  
				if(b.matches()&&c.matches()&&status){
					kt.set(advid+"\t"+arr[8]+"\t"+siteid+"\t"+arr[5]+"\t"+"cookie");
					context.write(kt, vt);
					kt.set(advid+"\t"+arr[8]+"\t"+siteid+"\t"+arr[6]+"\t"+"ip");
					context.write(kt, vt);
				}
			}
		}
	}
	
	/** 
	 * @function 分析媒体数据    Reduce阶段
	 * @output key : 厂商ID , 活动  , 媒体 ,  cookie/ip 
	 *       value :  sum
	 *
	 */
	public static class AnalyMediaReduce  extends Reducer <Text, IntWritable ,Text , IntWritable>{
        private Text kt = new Text();
        private IntWritable vt = new IntWritable();
        public void reduce (Text key , Iterable<IntWritable> values ,Context context)throws IOException , InterruptedException{
        	int sum = 0;
			for(IntWritable value : values){
				sum+=value.get();
			}
			String [] arr = key.toString().split("\t");
			kt.set(arr[0]+"\t"+arr[1]+"\t"+arr[2]+"\t"+arr[4]);
			vt.set(sum);
			context.write(kt, vt);
        }
	}
	
	
	/** 
	 * @function 计算媒体数据 Map 阶段
	 * @output key : 厂商 , 活动  , 媒体 , 
	 *       value : cookie/ip  sum
	 *
	 */
	public static class SumMediaMap extends Mapper<LongWritable, Text, Text, Text>{
		
		private Text kt = new Text();
		private Text vt = new Text();
		
		public void map (LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
			String arr[] = value.toString().split("\t");
			kt.set(arr[0]+"\t"+arr[1]+"\t"+arr[2]);
			vt.set(arr[3]+"\t"+arr[4]);
			context.write(kt, vt);
		}
	}
	
	
	/** 
	 * @function 计算媒体数据 Map 阶段
	 * @output key : 厂商 , 活动  , 媒体 , 
	 *       value : pv , uv , uip , imp/clk
	 *
	 */
	public static class CombineMediaMap extends Mapper<LongWritable, Text, Text, Text>{
		private Text kt = new Text();
		private Text vt = new Text();
		
		public void map (LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
			String [] arr = value.toString().split("\t");
			kt.set(arr[0]+"\t"+arr[1]+"\t"+arr[2]);
			vt.set(arr[3]+"\t"+arr[4]+"\t"+arr[5]+"\t"+arr[6]);
			context.write(kt, vt);
		}
	}
	
	/** 
	 * @function 计算活动数据 Map 阶段
	 * @output key : 厂商 , 活动  , cookie/ip
	 *       value: 1
	 *
	 */
	public static class AnalyOrderMap extends Mapper<LongWritable, Text, Text, IntWritable>{
			
			private Text kt = new Text();
			private IntWritable vt = new IntWritable(1);
			private String [] orderIDArr;
			
			protected void setup(Context context) throws IOException,
			InterruptedException {
				super.setup(context);
				orderIDArr = context.getConfiguration().get("orderID").split(",");
			}
			
			
			public void map (LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
				String [] arr = value.toString().split("\t");
					if(arr.length>8){
							String regex = "^[0-9]+$";
							String advid = arr[7];
							String orderid = arr[8];
							Matcher c = Pattern.compile(regex).matcher(advid);  
							boolean status = false;
							int len = orderIDArr.length;
							for(int i=0 ; i<len ; i++){
								if(orderIDArr[i].equals(orderid)){
									status  = true;
									break;
								}
							}
							if(c.matches()&&status){
								kt.set(advid+"\t"+orderid+"\t"+arr[6]+"\t"+"ip");
								context.write(kt, vt);
								kt.set(advid+"\t"+orderid+"\t"+arr[5]+"\t"+"cookie");
								context.write(kt, vt);
							}
						}
			}
	}
	
	
	/** 
	 * @function 计算活动数据 Map 阶段
	 * @output key : 厂商 , 活动  , cookie/ip
	 *       value: sum 
	 *
	 */
	public static class AnalyOrderReduce  extends Reducer<Text, IntWritable ,Text , IntWritable>{
        private Text kt = new Text();
        private IntWritable vt = new IntWritable();
        public void reduce (Text key , Iterable<IntWritable> values ,Context context)throws IOException , InterruptedException{
        	int sum = 0;
			for(IntWritable value : values){
				sum+=value.get();
			}
			String [] arr = key.toString().split("\t");
			kt.set(arr[0]+"\t"+arr[1]+"\t"+arr[3]);
			vt.set(sum);
			context.write(kt, vt);
        }
	}
	
	/** 
	 * @function 计算活动数据 Map 阶段
	 * @output key : 厂商 , 活动  
	 *       value: sum , cookie/ip
	 *
	 */
	public static class SumOrderMap extends Mapper<LongWritable, Text, Text, Text>{
		
		private Text kt = new Text();
		private Text vt = new Text();
		
		public void map (LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
			String arr[] = value.toString().split("\t");
			kt.set(arr[0]+"\t"+arr[1]);
			vt.set(arr[2]+"\t"+arr[3]);
			context.write(kt, vt);
		}
	}
	
	
	/** 
	 * @function 计算活动数据 Map 阶段
	 * @output key : 厂商  , 活动   
	 *       value : pv , uv , uip , imp/clk
	 *
	 */
	public static class CombineOrderMap extends Mapper<LongWritable, Text, Text, Text>{
		private Text kt = new Text();
		private Text vt = new Text();
		
		public void map (LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
			String [] arr = value.toString().split("\t");
			kt.set(arr[0]+"\t"+arr[1]);
			vt.set(arr[2]+"\t"+arr[3]+"\t"+arr[4]+"\t"+arr[5]);
			context.write(kt, vt);
		}
	}
	
	
	/** 
	 * @function 计算广告数据 Map阶段
	 * @output key : 厂商 , 活动 , 媒体 , 广告 , cookie/ip
	 *       value : 1
	 *
	 */
	public static class AnalyOrderItemMap extends Mapper<LongWritable, Text, Text, IntWritable>{
			
			private Text kt = new Text();
			private IntWritable vt = new IntWritable(1);
			
			private String [] orderIDArr ;
			
			/***
			 * 获取从JobControl端传递过来的日期和活动参数
			 */
			protected void setup(Context context) throws IOException,
			InterruptedException {
				super.setup(context);
				orderIDArr = context.getConfiguration().get("orderID").split(",");
			}
			
			public void map (LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
				String [] arr = value.toString().split("\t");
				if(arr.length>8){
					String advid = arr[7];
					String orderitemid = arr[2];
					String siteid = arr[3];
					String regex = "^[0-9]+$";
					Matcher a = Pattern.compile(regex).matcher(orderitemid);
					Matcher b = Pattern.compile(regex).matcher(siteid);
					Matcher c = Pattern.compile(regex).matcher(advid);
					int len = orderIDArr.length;
					boolean status = false;
					for(int i=0 ;i<len ;i++){
						if(orderIDArr[i].equals(arr[8])){
							status = true;
							break;
						}
					}
					if (a.matches() && b.matches()&&c.matches()&&status ) {
						kt.set(advid+"\t"+arr[8]+"\t"+siteid+"\t"+orderitemid+"\t"+arr[6]+"\t"+"ip");
						//orderitemid company orderid mediaid ip
						context.write(kt, vt);
						kt.set(advid+"\t"+arr[8]+"\t"+siteid+"\t"+orderitemid+"\t"+arr[5]+"\t"+"cookie");
						//orderitemid company orderid mediaid cookie
						context.write(kt, vt);
					}
				}
			}
	}
	
	/** 
	 * @function 计算媒体地域数据 Map阶段
	 * @output key :  厂商 , 活动 , 媒体 , 地域 ,  cookie/ip
	 *       value : 1
	 *
	 */
	public static class AnalyMediaAreaMap extends Mapper<LongWritable, Text, Text, IntWritable>{
		private Text kt = new Text();
		private IntWritable vt = new IntWritable(1);
		
		private String [] orderIDArr ;
		
		/***
		 * 获取从JobControl端传递过来的日期和活动参数
		 */
		protected void setup(Context context) throws IOException,
		InterruptedException {
			super.setup(context);
			orderIDArr = context.getConfiguration().get("orderID").split(",");
		}
		
		public void map (LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
			String [] arr = value.toString().split("\t");
			if(arr.length>8){
				int len  = orderIDArr.length;
				boolean status = false;
				for(int i=0 ; i<len ;i++){
					if(orderIDArr[i].equals(arr[8])){
						status = true;
						break;
					}
				}
				String regex = "^[0-9]+$";
				Matcher a = Pattern.compile(regex).matcher(arr[7]);
				Matcher b = Pattern.compile(regex).matcher(arr[3]);
				if (a.matches()&& b.matches()&&status) {
					kt.set(arr[7]+"\t"+arr[8]+"\t"+arr[3]+"\t"+arr[11]+"\t"+arr[6]+"\t"+"ip");
					//company orderid mediaid ip
					context.write(kt, vt);
					kt.set(arr[7]+"\t"+arr[8]+"\t"+arr[3]+"\t"+arr[11]+"\t"+arr[5]+"\t"+"cookie");
					//company orderid mediaid cookie 
					context.write(kt, vt);
				}
			}
		}
	}
	
	
	/** 
	 * @function 计算媒体地域数据 Map阶段
	 * @output key :  厂商 , 活动 , 媒体 , 地域   cookie/ip
	 *       value : sum
	 *
	 */
	public static class AnalyMediaAreaReduce  extends Reducer<Text, IntWritable ,Text , IntWritable>{
		
		private Text kt = new Text();
		private IntWritable vt = new IntWritable();
		
		public void reduce (Text key , Iterable<IntWritable> values ,Context context)throws IOException , InterruptedException{
			int sum = 0;
			for(IntWritable value : values){
				sum+=value.get();
			}
			String [] arr = key.toString().split("\t");
			kt.set(arr[0]+"\t"+arr[1]+"\t"+arr[2]+"\t"+arr[3]+"\t"+arr[5]);
			vt.set(sum);
			context.write(kt, vt);
		}
	}
	
	/** 
	 * @function 计算媒体地域数据 Map阶段
	 * @output key :  厂商 , 活动 , 媒体 , 地域 
	 *       value : cookie/ip , sum
	 *
	 */
	public static class SumMediaAreaMap extends Mapper<LongWritable, Text, Text, Text>{
		
		private Text kt = new Text();
		private Text vt = new Text();
		
		public void map (LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
			String arr[] = value.toString().split("\t");
			kt.set(arr[0]+"\t"+arr[1]+"\t"+arr[2]+"\t"+arr[3]);
			vt.set(arr[4]+"\t"+arr[5]);
			context.write(kt, vt);
		}
	}
	
	/** 
	 * @function 计算媒体地域数据 Map阶段
	 * @output key :  厂商 , 活动 , 媒体 ,地域 
	 *       value : pv , uv , vip . imp/clk
	 *
	 */
	public static class CombineMediaAreaMap extends Mapper<LongWritable, Text, Text, Text>{
		private Text kt = new Text();
		private Text vt = new Text();
		
		public void map (LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
			String [] arr = value.toString().split("\t");
			kt.set(arr[0]+"\t"+arr[1]+"\t"+arr[2]+"\t"+arr[3]);
			vt.set(arr[4]+"\t"+arr[5]+"\t"+arr[6]+"\t"+arr[7]);
			context.write(kt, vt);
		}
	}
	
	/**
	 * @function 分析指定页面数据
	 * @output  key:厂商 , 活动 , 页面 , cookie/ip
	 *         value: duration , pv
	 */
	public static class AnalyActMap extends Mapper<LongWritable, Text, Text, Text>{
		
		private Text kt = new Text();
		private Text vt = new Text();
		
		private String [] actIdArr ; 
		
		protected void setup(Context context) throws IOException,
		InterruptedException {
			super.setup(context);
			actIdArr = context.getConfiguration().get("actID").split(",");
		}
		
		public void map (LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
			String  [] arr = value.toString().split("\t");
			int arrLen = arr.length;
			if(arrLen>13){
				int actIdArrLen = actIdArr.length;
				boolean status = false;
				for(int i= 0 ;i <actIdArrLen ; i++){
					if(actIdArr[i].equals(arr[5])){
						status = true;
						break;
					}
				}
				if(status){
					kt.set(arr[5]+"\t"+arr[0]+"\t"+"cookie");
					//页面 , cookie 
					vt.set(arr[12]+"\t"+arr[13]);//浏览量
					context.write(kt, vt);
					
					
					kt.set(arr[5]+"\t"+arr[1]+"\t"+"ip");
					// 页面 , ip
					vt.set(0+"\t"+0);
					context.write(kt, vt);
				}
			}
		}
	}
	
	/**
	 * @function 分析指定页面数据
	 * @output key: 页面 , cookie/ip
	 *       value: visit ,  pv
	 */
	public static class AnalyActReduce extends Reducer<Text, Text ,Text , Text>{
		private Text kt = new Text();
		private Text vt = new Text();
		public void reduce (Text key , Iterable<Text> values ,Context context)throws IOException , InterruptedException{
			String arr[] = key.toString().split("\t");
			kt.set(arr[0]+"\t"+arr[2]);
			int visit =0 ; 
			double duration = 0;
			int pv =0 ;
			for(Text  value : values){
				String [] valueArr = value.toString().split("\t");
				duration+=Double.valueOf(valueArr[0]);
				pv += Integer.valueOf(valueArr[1]);
				visit++	;
			}
			vt.set(visit+"\t"+pv+"\t"+duration);
			context.write(kt, vt);
		}
	}
	
	/**
	 * @function 计算指定页面数据
	 * @output key: 厂商 , 活动 , 页面  
	 *       value: ip/cookie , visit , pv , duration 
	 *
	 */
	public static class SumActMap extends Mapper<LongWritable, Text, Text, Text>{
		private Text kt = new Text();
		private Text vt = new Text();
		
		public void map (LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
			String [] arr = value.toString().split("\t");
			kt.set(arr[0]);
			vt.set(arr[1]+"\t"+arr[2]+"\t"+arr[3]+"\t"+arr[4]);
			context.write(kt, vt);
		}
	}
	
	/**
	 * @function 计算指定页面数据
	 * @output key:页面
	 *       value: visit ,  pv , uv , uip
	 */
	public static class SumActReduce extends Reducer<Text, Text ,Text , Text>{
		private Text vt = new Text();
		
		private String metrics = null;
		protected void setup(Context context) throws IOException,
		InterruptedException {
			super.setup(context);
			metrics  = context.getConfiguration().get("metrics");
		}
		
		public void reduce (Text key , Iterable<Text> values ,Context context)throws IOException , InterruptedException{
			int pv = 0;
			double duration  = 0;
			int visit = 0;
			int uv = 0;
			int uip = 0;
			for(Text value : values){
				String arr[] = value.toString().split("\t");
				if("ip".equals(arr[0])){
				   uip++;	
				}else{
					pv += Integer.valueOf(arr[2]);
					visit += Integer.valueOf(arr[1]); 
					duration += Double.valueOf(arr[3]);
					uv ++;
				}
			}
			double avgDuration  =0 ;
			BigDecimal big = new BigDecimal(duration/visit);
			avgDuration = big.divide(new BigDecimal(1), 2, BigDecimal.ROUND_HALF_UP).doubleValue();
			StringBuffer sf = new StringBuffer();
			sf.append(visit)
			.append("\t")
			.append(uv)
			.append("\t")
			.append(uip)
			.append("\t")
			.append(avgDuration)
			.append("\t")
			.append(pv);
			vt.set(CommonUtil.beSureOutputValue(metrics, sf.toString()));
			context.write(key, vt);
		}
	}
	/**
	 * @function 分析页面地域数据
	 * @output  key: 页面 , 地域  ,  cookie/ip
	 *        value: pv , duration /0 , 0 
	 */
	public static class AnalyActRegionMap  extends Mapper<LongWritable, Text, Text, Text>{
		
		private Text kt = new Text();
		private Text vt = new Text();
		
		private String [] actIdArr  ; 
		
		protected void setup(Context context) throws IOException,
		InterruptedException {
			super.setup(context);
			actIdArr = context.getConfiguration().get("actID").split(",");
		}
		
		public void map (LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
			String  [] arr = value.toString().split("\t");
			int len = arr.length;
			if(len>23){
				int actIdArrLen = actIdArr.length;
				boolean status  = false;
				for(int i=0 ; i<actIdArrLen ; i++){
					if(actIdArr[i].equals(arr[5])){
						status = true;
						break;
					}
				}
				if(status){
					kt.set(arr[5]+"\t"+arr[23]+"\t"+arr[0]+"\t"+"cookie");
					// 页面 , 地域  ,  cookie 
					vt.set(arr[12]+"\t"+arr[13]);//浏览量
					context.write(kt, vt);
					kt.set(arr[5]+"\t"+arr[23]+"\t"+arr[1]+"\t"+"ip");
					// 页面  , 地域 ,  ip
					vt.set(0+"\t"+0);
					context.write(kt, vt);
				}
			}
		}
	}
	
	/**
	 * @function 分析指定页面数据
	 * @output key:页面 , 地域   , cookie/ip
	 *       value: visit ,  pv , duration 
	 */
	public static class AnalyActRegionReduce extends Reducer<Text, Text ,Text , Text>{
		private Text kt = new Text();
		private Text vt = new Text();
		public void reduce (Text key , Iterable<Text> values ,Context context)throws IOException , InterruptedException{
			String arr[] = key.toString().split("\t");
			kt.set(arr[0]+"\t"+arr[1]+"\t"+arr[3]);
			int visit =0 ; 
			double duration =0 ; 
			int pv =0 ;
			for(Text value : values){
				String [] valueArr = value.toString().split("\t");
				duration += Double.valueOf(valueArr[0]);
				pv+=Integer.valueOf(valueArr[1]);
				visit++;
			}
			vt.set(visit+"\t"+pv+"\t"+duration);
			context.write(kt, vt);
		}
	}
	
	/**
	 * @function 计算指定页面数据
	 * @output key:页面  , 地域
	 *       value: ip/cookie , visit , pv  , duration
	 *
	 */
	public static class SumActRegionMap extends Mapper<LongWritable, Text, Text, Text>{
		private Text kt = new Text();
		private Text vt = new Text();
		
		public void map (LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
			String [] arr = value.toString().split("\t");
			kt.set(arr[0]+"\t"+arr[1]);
			vt.set(arr[2]+"\t"+arr[3]+"\t"+arr[4]+"\t"+arr[5]);
			context.write(kt, vt);
		}
	}
	
	/**
	 * @function 分析厂商分小时浏览量
	 * @output key: 厂商
	 *      output:1      
	 *
	 */
	public static class AnalyCompanyHourMap extends Mapper<LongWritable, Text, Text, IntWritable>{
		private Text kt = new Text();
		private IntWritable vt = new IntWritable(1);
		private String companyID ;
		/***
		 * 获取从JobControl端传递过来的日期和活动参数
		 */
		protected void setup(Context context) throws IOException,
		InterruptedException {
			super.setup(context);
			companyID = context.getConfiguration().get("companyID");
		}
		
		public void map (LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
			String [] arr = value.toString().split("\t");
			int arrLen = arr.length;
				if(arrLen>22){
					String[] arrTime = arr[9].split(" ");
					if(arrTime.length == 2) {
						String hour = (arrTime[1]).split(":")[0];
						if(companyID.equals("0")){
							kt.set(arr[7]+"\t"+hour);//advid
							context.write(kt, vt);
						}else{
							if(companyID.equals(arr[7])){
								kt.set(arr[7]+"\t"+hour);//advid
								context.write(kt, vt);
							}
						}
					}
				}else if(arrLen>9){
					String[] arrTime = arr[9].split(" ");
					if(arrTime.length == 2) {
						String hour = (arrTime[1]).split(":")[0];
						if(companyID.equals("0")){
							String companyId = arr[7];
							String regex = "^[0-9]*$";
							Matcher m = Pattern.compile(regex).matcher(companyId);
							if(m.matches()){
								kt.set(arr[7]+"\t"+hour);//advid
								context.write(kt, vt);
							}
						}else{
							if(companyID.equals(arr[7])){
									kt.set(arr[7]+"\t"+hour);//advid
									context.write(kt, vt);
							}
						}
					}
				}
		}
	}
	
	/**
	 * @function 分析分小时浏览量 Reduce阶段
	 */
	public static class AnalyCompanyHourReduce extends Reducer<Text, IntWritable ,Text , IntWritable>{
		
		private IntWritable vt = new IntWritable();
		
		public void reduce (Text key , Iterable<IntWritable> values ,Context context)throws IOException , InterruptedException{
			int pv = 0;
			for(IntWritable value : values){
				pv+=value.get();
			}
			vt.set(pv);
			context.write(key, vt);
		}
	}
	
	/**
	 * @function 分析指定活动分小时浏览量
	 * @output key: comapny  , order 
	 *       value: 1 
	 *
	 */
	public static class AnalyOrderHourMap extends Mapper<LongWritable, Text, Text, IntWritable>{
	
		private Text kt = new Text();
		private IntWritable vt = new IntWritable(1);
		private String companyID;
		private String orderID;
		
		protected void setup(Context context) throws IOException,
		InterruptedException {
			super.setup(context);
			companyID  = context.getConfiguration().get("companyID");
			orderID = context.getConfiguration().get("orderID");
		}
		
		
		public void map (LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
			String [] arr = value.toString().split("\t");
				if(arr.length>22){
					String[] arrTime = arr[9].split(" ");
					if(arrTime.length == 2) {
						String hour = (arrTime[1]).split(":")[0];
						if("0".equals(companyID)&&"0".equals(orderID)){
							kt.set(arr[7]+"\t"+arr[8]+"\t"+hour);
							context.write(kt, vt);
						}else if(!"0".equals(companyID)&&"0".equals(orderID)){
							if(companyID.equals(arr[7])){
								kt.set(companyID+"\t"+arr[8]+"\t"+hour);
								context.write(kt, vt);
							}
						}else{
							if(orderID.equals(arr[8])){
								kt.set(arr[7]+"\t"+orderID+"\t"+hour);
								context.write(kt, vt);
							}
						}
					}
				}else if(arr.length>9){
					String[] arrTime = arr[9].split(" ");
					if(arrTime.length == 2) {
						String hour = (arrTime[1]).split(":")[0];
						if("0".equals(companyID)&&"0".equals(orderID)){
							String regex = "^[0-9]*$";
							String advid = arr[7];
							String orderid = arr[8];
							Matcher c = Pattern.compile(regex).matcher(advid);  
							Matcher d = Pattern.compile(regex).matcher(orderid); 
							if(c.matches()&&d.matches()){
								kt.set(advid+"\t"+orderid+"\t"+hour);
								context.write(kt, vt);
							}
						}else if(!"0".equals(companyID)&&"0".equals(orderID)){
							if(companyID.equals(arr[7])){
								String regex = "^[0-9]*$";
								String orderid = arr[8];
								Matcher d = Pattern.compile(regex).matcher(orderid); 
								if(d.matches()){
									kt.set(companyID+"\t"+orderid+"\t"+hour);
									context.write(kt, vt);
								}
							}
						}else{
							if(orderID.equals( arr[8])){
								String regex = "^[0-9]*$";
								String advid = arr[7];
								Matcher c = Pattern.compile(regex).matcher(advid);  
								if(c.matches()){
									kt.set(advid+"\t"+orderID+"\t"+hour);
									context.write(kt, vt);
								}
							}
						}
					}
				}
			}
		}
	
	
	/** 
	 * @function 分析媒体分小时数据 Map 阶段
	 * @output key : 厂商ID , 活动  , 媒体 , Hour
	 *       value : 1
	 *
	 */
	public static class AnalyMediaHourMap extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		private Text kt = new Text();
		private IntWritable vt = new IntWritable(1);
		private String [] orderIdArr ;
		
		protected void setup(Context context) throws IOException,
		InterruptedException {
			super.setup(context);
			orderIdArr = context.getConfiguration().get("orderID").split(",");
		}
		
		public void map (LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
			String line = value.toString();
			String [] arr = line.split("\t");
			int arrLen =  arr.length;
			if(arrLen>9){
				String[] arrTime = arr[9].split(" ");
				if(arrTime.length == 2){
					String hour = (arrTime[1]).split(":")[0];
					boolean status = false;
					int orderIdArrLen = orderIdArr.length;
					for(int i=0 ; i < orderIdArrLen ; i++){
						if(orderIdArr[i].equals(arr[8])){
							status = true;
							break;
						}
					}
					if(status){
						String regex = "^[0-9]*$";
						String siteid = arr[3];
						String advid = arr[7];
						Matcher b = Pattern.compile(regex).matcher(siteid);
						Matcher c = Pattern.compile(regex).matcher(advid);  
						if(b.matches()&&c.matches()){
							kt.set(advid+"\t"+arr[8]+"\t"+siteid+"\t"+hour);
							context.write(kt, vt);
						}
					}
				}
			}
		}
	}
	
	
	/**
	 * @function 分析指定页面分小时数据
	 * @output  key:厂商 , 活动 , 页面 , hour
	 *         value: pv
	 */
	public static class AnalyActHourMap extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		private Text kt = new Text();
		private IntWritable vt = new IntWritable(1);
		private String actID ; 
		
		protected void setup(Context context) throws IOException,
		InterruptedException {
			super.setup(context);
			actID = context.getConfiguration().get("actID");
		}
		
		public void map (LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
			String  [] arr = value.toString().split("\t");
			int len = arr.length;
			if(len>9){
				if("actID".equals(arr[9])){
					String[] arrTime = arr[6].split(" ");
					if(arrTime.length == 2) {
						String hour = (arrTime[1]).split(":")[0];
						kt.set(actID+"\t"+hour);
						context.write(kt, vt);
					}
				}
			}
		}
	}
	
	/**
	 * @function 厂商 , 活动 , 人群
	 * @output  key:厂商 , 活动 , 人群 , cookie/ip
	 *       output: 1
	 *
	 */
	public static class AnalyOrderImpCrowdMap extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		private Text kt = new Text();
		private IntWritable vt = new IntWritable(1);
		
		private String [] orderIDArr ;
		
		protected void setup(Context context) throws IOException,
		InterruptedException {
			super.setup(context);
			orderIDArr = context.getConfiguration().get("orderID").split(",");
		}
		
		public void map (LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
			String [] rowArr  = value.toString().split("\t");
			int rowLen = rowArr.length;
			if(rowLen>15){
				boolean status = false;
				int orderIDArrLen = orderIDArr.length;
				String orderId = rowArr[8].trim();
				for(int i=0 ; i<orderIDArrLen ; i++){
					if(orderIDArr[i].equals(orderId)){
						status = true;
						break;
					}
				}
				if(status){
					String advid = "".equals(rowArr[7])?"0":rowArr[7];//厂商ID
					String cookie = "".equals(rowArr[5])?"0":rowArr[5];//cookie
			    	kt.set(advid+"\t"+orderId+"\t"+cookie);
			    	context.write(kt, vt);
				}
			}
		}
	}
	
	public static class AnalyOrderImpCrowdReduce extends Reducer<Text, IntWritable ,Text , Text>{
		
		private Text outKey = new Text();
		private Text outValue = new Text(); 
		public void reduce (Text key , Iterable<IntWritable> values ,Context context)throws IOException , InterruptedException{
			int pv = 0;
			for(IntWritable value : values) {
				pv += value.get();
			}
			outValue.set(pv + "\t" + 1); //PV	UV
			String[] arrKeys = key.toString().split("\t");
			String cookies = arrKeys[2];
			Set<String> crowds = RedisUtil.hKeys(cookies);
			for(String crowd : crowds) {
				outKey.set(arrKeys[0] + "\t" + arrKeys[1] + "\t" + crowd); // advid orderid crowd(人群ID)
				context.write(outKey, outValue);
			}
		}
	}
	
	public static class sumCrowdMap extends Mapper<LongWritable, Text, Text, Text>{
		
		private Text kt = new Text();
		private Text vt = new Text();
		
		public void map (LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
			String arr[] = value.toString().split("\t");
			kt.set(arr[0]+"\t"+arr[1]+"\t"+arr[2]); // advid orderid crowd(人群ID)
			vt.set(arr[3]+"\t"+arr[4]);//PV	UV
			context.write(kt, vt);
		}
	}
	
	public static class sumCrowdReduce extends Reducer<Text, Text ,Text , Text>{
		
		private Text vt = new Text();
		private String impOrClk ;
		
		/***
		 * 获取从JobControl端传递过来的日期和活动参数
		 */
		protected void setup(Context context) throws IOException,
		InterruptedException {
			super.setup(context);
			impOrClk  = context.getConfiguration().get("impOrClk");
		}
		
		public void reduce (Text key , Iterable<Text> values ,Context context)throws IOException , InterruptedException{
			int pv = 0;
			int uv = 0;
			for(Text value : values) {
				String[] arrValue = value.toString().split("\t");
				pv += Integer.parseInt(arrValue[0]);
				uv += Integer.parseInt(arrValue[1]);
			}
			vt.set(pv+"\t"+uv+"\t"+0+"\t"+impOrClk);
			context.write(key, vt);
		}
}
	/**
	 * @function 厂商 , 活动 , 人群
	 * @output  key:厂商 , 活动 , 人群 , cookie/ip
	 *       output: 1
	 */
	public static class AnalyOrderClkCrowdMap extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		private Text kt = new Text();
		private IntWritable vt = new IntWritable(1);
		private String [] orderIDArr ;
		
		protected void setup(Context context) throws IOException,
		InterruptedException {
			super.setup(context);
			orderIDArr = context.getConfiguration().get("orderID").split(",");
		}
		
		public void map (LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
			String [] rowArr  = value.toString().split("\t");
			int rowLen = rowArr.length;
			if(rowLen>15){
				String bits = rowArr[15].trim();
				if(!"".equals(bits)&&!bits.contains(":")){
					boolean status = false;
					int orderIDArrLen = orderIDArr.length;
					for(int i=0 ;i<orderIDArrLen ; i++){
						if(orderIDArr[i].equals(rowArr[3])){
							status = true;
							break;
						}
					}
					if(status){
						String advid = "".equals(rowArr[1])?"0":rowArr[1];//厂商ID
						String cookie = "".equals(rowArr[0])?"0":rowArr[0];//cookie
						String ip = "".equals(rowArr[10])?"0":rowArr[10];//ip
					    String [] bitsArr = bits.split(",");
					    int bitsArrLen = bitsArr.length;
					    for(int i=0 ;i<bitsArrLen ; i++){
					    	kt.set(advid+"\t"+rowArr[3]+"\t"+bitsArr[i]+"\t"+cookie+"\t"+"cookie");
					    	context.write(kt, vt);
					    	kt.set(advid+"\t"+rowArr[3]+"\t"+bitsArr[i]+"\t"+ip+"\t"+"ip");
					    	context.write(kt, vt);
					    }
					}
				}
			}
		}
	}
	
	/**
	 * @function 分析指定活动下的媒体重合度数据
	 * @output key: company , orderid , cookie 
	 *       value: media
	 *
	 */
	public static class AnalyOrderSiteCoincideMap  extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		private Text kt = new Text();
		private IntWritable vt = new IntWritable();
		private String companyID;
		private String [] orderIDArr;
		
		protected void setup(Context context) throws IOException,
		InterruptedException {
			super.setup(context);
			orderIDArr = context.getConfiguration().get("orderID").split(",");
		}
		
		public void map (LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
			String line = value.toString();
			String [] arr = line.split("\t");
			int arrLen =  arr.length;
			if(arrLen>8){
				int len = orderIDArr.length;
				boolean status = false;
				for(int i=0 ; i<len ; i++){
					if(orderIDArr[i].equals(arr[8])){
						status = true;
						break;
					}
				}
				String regex = "^[0-9]+$";
				String siteid = arr[3];
				String advid = arr[7];
				Matcher b = Pattern.compile(regex).matcher(siteid);
				Matcher c = Pattern.compile(regex).matcher(advid);  
				if(b.matches()&&c.matches()&&status){
					kt.set(advid+"\t"+arr[8]+"\t"+arr[5]);
					vt.set(Integer.valueOf(siteid));
					context.write(kt, vt);
				}
			}
		}
	}
	
	/***
	 * @function 活动下的媒体重合度  Reduce 阶段
	 * @output  siteid_1  siteid_2  
	 */
	public static  class AnalyOrderSiteCoincideReduce  extends Reducer<Text, IntWritable ,Text , Text>{
		
		private Text kt = new Text();
		private Text vt = new Text();
		
		private String coincide = null;
		
		protected void setup(Context context) throws IOException,
		InterruptedException {
			super.setup(context);
			coincide = context.getConfiguration().get("coincide");
		}
		
		
		public void reduce (Text key , Iterable<IntWritable> values ,Context context)throws IOException , InterruptedException{
			String arr[] = key.toString().split("\t");
			kt.set(arr[0]+"\t"+arr[1]);
			Set<Integer> siteIdSet = new HashSet<Integer>();
			for(IntWritable value : values){
				siteIdSet.add(value.get());
			}
			int [] siteIdArr =  new int[siteIdSet.size()];
			int i = 0 ;
			Iterator<Integer> siteIdIter = siteIdSet.iterator();
			while(siteIdIter.hasNext()){
				siteIdArr[i] = siteIdIter.next();
				i++;
			}
			int arrLen = siteIdArr.length;
			Arrays.sort(siteIdArr);
			if("2".equals(coincide)){
				for(int x= 0 ; x<arrLen ; x++){
					for(int z=x+1 ; z<arrLen ; z++){
						StringBuffer sf = new StringBuffer();
						sf.append(siteIdArr[x]).append("\t").append(siteIdArr[z]);
						vt.set(sf.toString());
						context.write(kt, vt);
					}
				}
			}else if("3".equals(coincide)){
				for(int x= 0 ; x<arrLen ; x++){
					for(int z=x+1 ; z<arrLen ; z++){
						for(int y=z+1 ; y<arrLen ; y++){
							StringBuffer sf = new StringBuffer();
							sf.append(siteIdArr[x]).append("\t").append(siteIdArr[z]).append("\t").append(siteIdArr[y]);
							vt.set(sf.toString());
							context.write(kt, vt);
						}
					}
				}
			}else if("4".equals(coincide)){
				for(int x= 0 ; x<arrLen ; x++){
					for(int z=x+1 ; z<arrLen ; z++){
						for(int y=z+1 ; y<arrLen ; y++){
							for(int a = y+1 ; a<arrLen ; a++){
								StringBuffer sf = new StringBuffer();
								sf.append(siteIdArr[x]).append("\t").append(siteIdArr[z]).append("\t").append(siteIdArr[y]).append("\t").append(siteIdArr[a]);
								vt.set(sf.toString());
								context.write(kt, vt);
							}
						}
					}
				}
			}else if("5".equals(coincide)){
				for(int x= 0 ; x<arrLen ; x++){
					for(int z=x+1 ; z<arrLen ; z++){
						for(int y=z+1 ; y<arrLen ; y++){
							for(int a = y+1 ; a<arrLen ; a++){
								for(int b = a+1 ; b<arrLen ; b++){
									StringBuffer sf = new StringBuffer();
									sf.append(siteIdArr[x]).append("\t").append(siteIdArr[z]).append("\t").append(siteIdArr[y]).append("\t").append(siteIdArr[a]).append("\t").append(siteIdArr[b]);
									vt.set(sf.toString());
									context.write(kt, vt);
								}
							}
						}
					}
				}
			}
		}
	}
	
	
	/**
	 * @function 计算活动下媒体重合度 MAP 阶段
	 * @output siteId_1 siteId_2
	 *
	 */
	public static  class SumOrderSiteCoincideMap extends Mapper<LongWritable, Text, Text, IntWritable>{
		private Text kt = new Text();
		private IntWritable vt = new IntWritable(1);
		public void map (LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
			String line  = value.toString();
			kt.set(line);
			context.write(kt, vt);
		}
	}
	
	/**
	 * 
	 * @function 计算活动下媒体重合度 MAP 阶段
	 * @output siteId_1 siteId_2  uv
	 *
	 */
	public static  class SumOrderSiteCoincideReduce  extends Reducer<Text, IntWritable ,Text , IntWritable>{
		
		private IntWritable vt = new IntWritable();
		
		public void reduce (Text key , Iterable<IntWritable> values ,Context context)throws IOException , InterruptedException{
			int sum = 0;
			for(IntWritable value : values){
				sum += value.get();
			}
			vt.set(sum);
			context.write(key, vt);
		}
	}
	
	/**
	 * @function 分析页面人群数据     MAP阶段
	 * @output key: 厂商  , 页面 , 媒体 , 人群
	 *       vlaue :cookie , ip , 页面平均停留时间 , 浏览量
	 *
	 */
	public static class AnalyActCrowdMap extends Mapper<LongWritable, Text, Text, Text>{
		
		private Text kt = new Text ();
		private Text vt = new Text();
		
		private String [] actIdArr;
		
		protected void setup(Context context) throws IOException,
		InterruptedException {
			super.setup(context);
			actIdArr = context.getConfiguration().get("actId").split(",");
		}
		
		public void map (LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
			String [] arr = value.toString().split("\t");
			int len = arr.length;
			if(len == 12){
				boolean status = false;
				int actIdArrLen =  actIdArr.length;
				for(int i= 0; i<actIdArrLen ; i++){
					if(actIdArr[i].equals(arr[1])){
						status = true;
						break;
					}
				}
				if(status){
					kt.set(arr[2]+"\t"+arr[1]+"\t"+arr[4]+"\t"+arr[11]);
					//advid actid siteid bits
					vt.set(arr[0]+"\t"+arr[1]+"\t"+arr[6]+"\t"+arr[7]);
					//cookie ip duration pv
					context.write(kt, vt);
				}
			}
		}
	}
	
	/**
	 * @function 分析页面人群数据     REDUCE阶段
	 * @output key: 厂商  , 页面 , 媒体 , 人群
	 *       vlaue :Visit , UV , UIP , AvgDuration , PV
	 *
	 */
	public static class AnalyActCrowdReduce extends Reducer<Text, Text ,Text , Text>{
		
		private Text vt = new Text();
		
		private String metrics = null;
		protected void setup(Context context) throws IOException,
		InterruptedException {
			super.setup(context);
			metrics  = context.getConfiguration().get("metrics");
		}
		
		public void reduce (Text key , Iterable<Text> values ,Context context)throws IOException , InterruptedException{
			List<String> all_value = new ArrayList<String>();
			for(Text value : values){//循环输入values集合中的数据
				all_value.add(String.valueOf(value));
			}
			int visit = all_value.size();
			String [] arr_first = new String[visit];
			Iterator<String> iter_value = all_value.iterator();
			int i=0;
			while(iter_value.hasNext()){
				arr_first[i] = iter_value.next();
				i++;
			}
			String need_value = CommonUtil.calCulateValue(arr_first);
			vt.set(CommonUtil.beSureOutputValue(metrics, need_value));
			context.write(key, vt);
		}
	}
	
	
	/**
	 * 
	*   
	* 类名称：AnalyOrderFrequencyMap  
	* 类描述： 分析指定活动下的频次数据
	* 创建人：yang qi  
	* 创建时间：2012-8-13 上午11:43:58  
	* 修改人：yang qi  
	* 修改时间：2012-8-13 上午11:43:58  
	* 修改备注：  
	* @version   
	*
	 */
	public static class AnalyOrderFrequencyMap extends Mapper<LongWritable, Text, Text, IntWritable>{
		private Text kt = new Text();
		private IntWritable vt = new IntWritable(1);
		private String [] orderIdArr ;
		
		/***
		 * 获取从JobControl端传递过来的活动ID参数
		 */
		protected void setup(Context context) throws IOException,
		InterruptedException {
			super.setup(context);
			orderIdArr = context.getConfiguration().get("orderId").split(",");
		}
		public void map (LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
			String [] arr = value.toString().split("\t");
			int arrLength = arr.length;
			if(arrLength>8){
				int orderIdArrLen  = orderIdArr.length;
				boolean status = false;
				for(int i=0;i<orderIdArrLen ;i++){
					if(orderIdArr[i].equals(arr[8])){
						status = true;
						break;
					}
				}
				if(status){
					String cookie = arr[5];
					if("".equals(cookie)){
						cookie = "NULL";
					}
					kt.set(arr[8]+"\t"+cookie);
					context.write(kt, vt);
				}
			}
		}
	}
	
	/**
	 *@function 分析指定活动下频次PV UV数据 REDUCE阶段
	 *@output  orderid  pv(cookie)
	 *
	 */
	public static class AnalyOrderFrequencyReduce extends Reducer<Text, IntWritable ,Text , IntWritable >{
		
		private Text kt = new Text();
		private IntWritable vt = new IntWritable();
		
		public void reduce (Text key , Iterable<IntWritable> values ,Context context)throws IOException , InterruptedException{
			String [] keyArr = key.toString().split("\t");
			kt.set(keyArr[0]);
			int sum = 0;
			for(IntWritable value : values){
				sum+=value.get();
			}
			vt.set(sum);
			context.write(kt, vt);
		}
	}
	
	/**
	 *@function 计算指定活动下频次PV UV数据 MAP阶段
	 *@output  orderid  pv(cookie)
	 */
	public static class SumOrderFrequencyMap extends Mapper<LongWritable, Text, Text, IntWritable>{
		private Text kt = new Text();
		private IntWritable vt = new IntWritable();
		private int frequency ;
		
		protected void setup(Context context) throws IOException,
		InterruptedException {
			super.setup(context);
			frequency = Integer.valueOf(context.getConfiguration().get("frequency"));
		}
		
		
		public void map (LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
			String [] arr = value.toString().split("\t");
			int i = Integer.valueOf(arr[1]);
			kt.set(arr[0]+"\t"+(i>frequency?frequency:i));
			vt.set(i);
			context.write(kt, vt);
		}
	}
	
	
	/**
	 *@function 计算指定活动下频次PV UV数据 REDUCE阶段
	 *@output  orderid frequency pv(cookie) uv(cookie)
	 *
	 */
	public static class SumOrderFrequencyReduce extends Reducer<Text, IntWritable ,Text , Text >{
		
		Text vt = new Text();
		private String impOrClk ;
		
		protected void setup(Context context) throws IOException,InterruptedException {
			super.setup(context);
			impOrClk = context.getConfiguration().get("impOrClk");
		}
		
		public void reduce (Text key , Iterable<IntWritable> values ,Context context)throws IOException , InterruptedException{
			int sum = 0;
			int uv = 0;
			for(IntWritable value : values){
				sum += value.get();
				uv++;
			}
			vt.set(sum+"\t"+uv+"\t"+impOrClk);
			context.write(key, vt);
		}
	}
	
	
	/**
	 *@function 分析指定活动下的媒体频次数据 MAP阶段
	 *@output orderid  siteid cookie 1
	 *
	 */
	public static class AnalyOrderSiteFrequencyMap extends Mapper<LongWritable, Text, Text,IntWritable>{
		private Text kt = new Text();
		private IntWritable vt= new IntWritable(1);
		private String [] orderIdArr ;
		
		/***
		 * 获取从JobControl端传递过来的活动ID参数
		 */
		protected void setup(Context context) throws IOException,
		InterruptedException {
			super.setup(context);
			orderIdArr = context.getConfiguration().get("orderId").split(",");
		}
		
		public void map (LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
			String [] arr = value.toString().split("\t");
			int arrLength = arr.length;
			if(arrLength>8){
				boolean status = false;
				int orderIdArrLen = orderIdArr.length;
				for(int i=0 ; i<orderIdArrLen ; i++){
					if(orderIdArr[i].equals(arr[8])){
						status = true;
						break;
					}
				}
				if(status){
					String siteid = arr[3];
					if(!"".equals(siteid)){
						String cookie = arr[5];
						if("".equals(cookie)){
							cookie = "NULL";
						}
						kt.set(arr[8]+"\t"+siteid+"\t"+cookie);
						context.write(kt, vt);
					}
				}
			}
		}
	}
	
	
	/**
	 *@function 分析指定活动下的媒体频次数据 REDUCE阶段
	 *@output  orderid siteid  PV(cookie)
	 *
	 */
	public static class AnalyOrderSiteFrequencyReduce extends Reducer<Text, IntWritable ,Text , IntWritable >{
		
		private Text kt = new Text();
		private IntWritable vt = new IntWritable();
		
		public void reduce (Text key , Iterable<IntWritable> values ,Context context)throws IOException , InterruptedException{
			String [] keyArr = key.toString().split("\t");
			kt.set(keyArr[0]+"\t"+keyArr[1]);
			int sum = 0;
			for(IntWritable value : values){
				sum+=value.get();
			}
			vt.set(sum);
			context.write(kt, vt);
		}
	}
	
	
	/**
	 * 
	 * @function 计算大类频次报表 REDUCE阶段
	 * @output 活动ID , 媒体ID , 频次 , PV
	 */
	public static class SumOrderSiteFrequencyMap extends Mapper<LongWritable, Text, Text, IntWritable>{
		private Text kt = new Text();
		private IntWritable vt = new IntWritable();
		
		private int frequency ;
		
		protected void setup(Context context) throws IOException,
		InterruptedException {
			super.setup(context);
			frequency = Integer.valueOf(context.getConfiguration().get("frequency"));
		}
		
		public void map (LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
			String [] arr = value.toString().split("\t");
			int i = Integer.valueOf(arr[2]);
			kt.set(arr[0]+"\t"+arr[1]+"\t"+(i>frequency?frequency:i));
			vt.set(i);
			context.write(kt, vt);
		}
	}
	
	
	/**
	 * @function 分析广告来源曝光量或点击量
	 * @output key: 厂商 , 活动 , 来源主域名
	 *      output: 1
	 *
	 */
	public static class AnalyOrderItemReferralMap extends Mapper<LongWritable, Text, Text, IntWritable>{
		private Text kt = new Text();
		private IntWritable vt = new IntWritable(1);
		private String  [] orderIdArr ;
		private String  urlType ;
		/***
		 * 获取从JobControl端传递过来的活动ID参数
		 */
		protected void setup(Context context) throws IOException,
		InterruptedException {
			super.setup(context);
			orderIdArr = context.getConfiguration().get("orderId").split(",");
			urlType = context.getConfiguration().get("urlType");
		}
		
		public void map (LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
			String arr[] = value.toString().split("\t");
			int arrLen = arr.length;
			if(arrLen>8){
				boolean status = false;
				int orderIdArrLen = orderIdArr.length;
				for(int i=0 ; i<orderIdArrLen ; i++){
					if(orderIdArr[i].equals(arr[8])){
						status = true;
						break;
					}
				}
				if(status){
					String referral = arr[13];
//					if(!"".equals(referral)&&referral.contains("http://")){
//						if("1".equals(urlType)){//域名形式（1）
//							String url ;
//							int b = referral.indexOf("/", 7);
//							if(b==-1){
//								url = referral;
//							}else{
//								url = referral.substring(0, b);
//							}
//							kt.set(arr[7]+"\t"+arr[8]+"\t"+arr[3]+"\t"+arr[2]+"\t"+url);
//						}else if("2".equals(urlType)){//包含网页完整地址（2） 
//							kt.set(arr[7]+"\t"+arr[8]+"\t"+arr[3]+"\t"+arr[2]+"\t"+referral);
//						}
//						context.write(kt, vt);
//					}
					if ("".equals(referral)) {
						referral = "无来源";
					}
					if ("1".equals(urlType)) {// 域名形式（1）
						String url;
						int b = referral.indexOf("/", 7);
						if (b == -1) {
							url = referral;
						} else {
							url = referral.substring(0, b);
						}
						kt.set(arr[7] + "\t" + arr[8] + "\t" + arr[3] + "\t"
								+ arr[2] + "\t" + url);
					} else if ("2".equals(urlType)) {// 包含网页完整地址（2）
						kt.set(arr[7] + "\t" + arr[8] + "\t" + arr[3] + "\t"
								+ arr[2] + "\t" + referral);
					}
					context.write(kt, vt);
				}
			}
		}
	}
	
	
	/**
	 * @function 分析大类小时报表 REDUCE阶段
	 * @output 厂商 , 活动 , 来源主域名 ,  pv/clk
	 * @修改author zhou yuanlong
	 * @修改date 2013-07-05
	 * @修改原因 增加包含网页完整地址功能 及优化代码
	 */
	public static class  AnalyOrderItemReferralReduce extends Reducer<Text, IntWritable ,Text , IntWritable >{
		
//		private Text kt = new Text();//2013-03-07 zhou.yuanlong
		private IntWritable vt = new IntWritable();
		
		public void reduce (Text key , Iterable<IntWritable> values ,Context context)throws IOException , InterruptedException{
			int sum =0 ;
			for(IntWritable value : values){
				sum += value.get();
			}
			vt.set(sum);
			
			//2013-03-07 zhou.yuanlong
			//因为添加了唯一展现数和唯一点击数 故需要加cookie，即需要在此切分key
//			String arr[] = key.toString().split("\t");
//			kt.set(arr[0] + "\t" + arr[1] + "\t" + arr[2] + "\t" + arr[3] + "\t" + arr[4]);
			context.write(key, vt);
		}
	}
	
	/**
	 * @function 计算指定活动下的 MAP阶段
	 * @output 
	 * @author zhou yuanlong
	 * @time 2013-02-18
	 */
	public static class SumOrderRegionPvAndUvMap extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private Text kt = new Text();
		private IntWritable vt = new IntWritable(1);

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] arr = value.toString().split("\t");
				
			kt.set(arr[0] + "\t" + arr[1] + "\t" + arr[2]+ "\t" + arr[3]+ "\t" + arr[4]);//cookie前5个key
			vt.set(Integer.valueOf(arr[5]));
				
			context.write(kt, vt);
		}
	}

	/**
	 * @function 计算指定活动下的 REDUCE阶段
	 * @output 
	 * @author zhou yuanlong
	 * @time 2013-02-18
	 */
	public static class SumOrderRegionPvAndUvReduce extends
			Reducer<Text, IntWritable, Text, Text> {

		private Text vt = new Text();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int uv = 0;
			int pv = 0;
			for (IntWritable value : values) {
				pv += value.get();
				uv++;
			}
			vt.set(pv + "\t" + uv);
			context.write(key, vt);
		}
	}
	
	/**
	 *@function 分析饱和度提取新增cookie Map阶段
	 *@output KEY: cookie  VALUE: 0/1(区别该cookie出现的日期)
	 */
	public static class AnalyseSaturationMap extends Mapper<LongWritable, Text, Text, Text>{
		
		//private String date ;
		private List<String> orderIds  ;
		private Text kt = new Text();
		private Text vt = new Text();
		private String endDate = "";
		
		/***
		 * 获取从JobControl端传递过来的日期和活动参数
		 */
		protected void setup(Context context) throws IOException,
		InterruptedException {
			super.setup(context);
			orderIds = Arrays.asList(context.getConfiguration().get("orderId").split(","));
			endDate = context.getConfiguration().get("endDate").trim();
		}
		
		public void map (LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
			String [] arr =  value.toString().split("\t");//按占位符拆分为数组
			if(arr.length>9){
				String orderId = arr[8].trim();
				if(orderIds.contains(orderId)){//日志记录属于指定广告活动
					String cookie = arr[5];//Cookie值
					String date = arr[9].trim().substring(0, 10);//时间
					String siteId  = arr[3];//媒体ID
					kt.set(date+"\t"+orderId+"\t"+siteId + "\t" + cookie);
					vt.set("date"); //这表示是当天日志的cookie
					context.write(kt, vt);
					while(!endDate.equals(date)) {
						date = DateUtil.strToDate(date, "明天");
						kt.set(date+"\t"+orderId+"\t"+siteId + "\t" + cookie);
						vt.set("exist");  //这表示在历史出现的过得cookie
						context.write(kt, vt);
					}
				}
				
			}
		}
	}
	
	
	/**
	 * @function 分析饱和度输出之前未出现的Cookie  Reduce阶段
	 * @output KEY: cookie 输出从广告监测开始到现在新出现的cookie VALUE: NULL
	 */
	public static class AnalyseSaturationReduce extends Reducer<Text, Text ,Text , NullWritable>{
		private Text outKey = new Text();
		private IntWritable outValue = new IntWritable(1);
		public void reduce (Text key , Iterable<Text> values ,Context context)throws IOException , InterruptedException{
			boolean isExist = false;
			for(Text value: values){
				if("exist".equals(value.toString().trim())) {
					isExist = true;
					break;
				}
			}
			
			if(!isExist) {
				String[] split_key = key.toString().split("\t");
				outKey.set(split_key[0] +"\t" + split_key[1] +"\t" +split_key[2]);
				context.write(outKey, NullWritable.get());
			}
			
		}
	}
	
	
	
	/**
	 * @function 计算从广告监测开始到现在新出现的cookie的总量 Map阶段
	 * @output KEY: 媒体ID (起一标记的作用) VALUE 1;
	 *
	 */
	public static class  SumSaturationMap extends Mapper<LongWritable, Text, Text, IntWritable>{
		private Text kt = new Text();
		private IntWritable vt = new IntWritable(1);
		
		public void map (LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
			String[] arrValue = value.toString().split("\t");
			context.write(value, vt);
		}
		
	}
	
	
	
	/**
	 * @function 计算从广告监测开始到现在新出现的cookie的总量 Reduce阶段
	 * @output KEY: sum (饱和度值) VALUE: NULL;
	 */
	public static class SumSaturationReduce extends Reducer<Text, IntWritable ,Text	 , IntWritable>{
		private IntWritable vt = new IntWritable();
		public void reduce (Text key , Iterable<IntWritable> values ,Context context)throws IOException , InterruptedException{
			int uv =0;
			for(IntWritable value : values){
				uv+=value.get();
			}
			vt.set(uv);
			context.write(key, vt);
		}
	}
	
	
	/**
	 * @function 分析出指定活动下的所有cookie MAP阶段
	 * @output key: cookie 
	 *      output: null
	 *
	 */
	public static class AnalyOrderCookieMap extends Mapper<LongWritable, Text, Text, NullWritable>{
		
		private Text kt = new Text();
		
		private String orderID ;
		
		/***
		 * 获取从JobControl端传递过来的活动ID参数
		 */
		protected void setup(Context context) throws IOException,
		InterruptedException {
			super.setup(context);
			orderID = context.getConfiguration().get("orderId");
		}
		
		public void map (LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
			String [] arr = value.toString().split("\t");//按占位符拆分行
			int len = arr.length;
			if(len>8){
				if(orderID.equals(arr[8])){//判断活动ID是否相等
					String cookie = arr[5];//cookie
					if(!"".equals(cookie)){
						kt.set(cookie);
						context.write(kt, NullWritable.get());
					}
				}
			}
		}
	}
	
	
	
	/**
	 * @function 分析出指定活动下的所有cookie Reduce阶段
	 * @output key: cookie 
	 *      output: null
	 *
	 */
	public static class AnalyOrderCookieReduce extends Reducer<Text, NullWritable ,Text , NullWritable >{
		
		public void reduce (Text key , Iterable<NullWritable> values ,Context context)throws IOException , InterruptedException{
			context.write(key, NullWritable.get());
		}
	}
	
	/**
	 * @function 分析用户浏览习惯 MAP阶段
	 * @ouptut key: cookie 
	 * 		 value: domain/"cookie"
	 *
	 */
	public static class AnalyBrowsingHabitsMap extends Mapper<LongWritable, Text, Text, Text>{
		 
		private Text kt = new Text();
		private Text vt = new Text();
		private String [] siteids ;
		
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			siteids = context.getConfiguration().get("siteids").split(",");
		}
		
		public void map (LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
			String [] arr = value.toString().split("\t");
			int len = arr.length;
			if(len == 1){
				kt.set(arr[0]);//cookie
				vt.set("cookie");
				context.write(kt, vt);
			}else if(len>14){
				boolean flag = false;
				for(int i=0 ; i<siteids.length ; i++){
					if(siteids[i].equals(arr[3])){
						flag = true;
						break;
					}
				}
				if(!flag){
					kt.set(arr[5]);
					String url = arr[13];//local
					if(!"".equals(url)&&url.contains("http://")){
						String domain ;
						int b = url.indexOf("/", 7);
						if(b==-1){
							domain = url;
						}else{
							domain = url.substring(0, b);
						}
						vt.set(domain);
						context.write(kt, vt);
					}
				}
			}
		}
	}
	
	/**
	 * @function 分析用户浏览习惯 Reduce阶段
	 * @ouptut key: cookie 
	 * 		 value: domain/"cookie"
	 *
	 */
	public static class AnalyBrowsingHabitsReduce extends Reducer<Text, Text ,Text , NullWritable >{
		
		private Text kt = new Text();
		
		public void reduce (Text key , Iterable<Text> values ,Context context)throws IOException , InterruptedException{
			int i = 0 ; 
			List PresentCookieSet = new ArrayList();
			for(Text value : values){
				String valueToString = value.toString();
				if("cookie".equals(valueToString)){
					i++;
				}else {
					PresentCookieSet.add(valueToString);
				}
			}
			if(i==0&&PresentCookieSet.size()!=0){
				Iterator iter = PresentCookieSet.iterator();
				while(iter.hasNext()){
					kt.set(String.valueOf(iter.next()));
					context.write(kt, NullWritable.get());
				}
			}
		}
	}
	
	
	/**
	 * 
	*   
	* 类名称：AnalyInventoryFrequencyMap  
	* 类描述： 分析指定活动下的媒体广告位频次数据
	* 创建人：zhou.yuanlong
	* 创建时间：2013-01-21 上午
	* 修改人 zhou.yuanlong
	* @date 第一次修改 2013-01-30
	* 修改备注：  
	* @date 第二次修改 2013-07-03
	* 修改备注：
	* 需求：广告位频次报告中在按照广告位进行汇总计算频次数据的时候，希望在输入筛选条件选择好广告位之后，筛选条件不仅仅局限于一个相同广告位名称的广告位ID
	* @version   
	*
	 */
	public static class AnalyInventoryFrequencyMap extends Mapper<LongWritable, Text, Text, IntWritable>{
		private Text kt = new Text();
		private IntWritable vt = new IntWritable(1);
		private String [] orderIdArr ;
		private String [] mediaArr ;//媒体
		private String [] inventoryidArr ;//广告位
		private String  collectArr ;//是否汇总  1是  0否
		
		/***
		 * 获取从JobControl端传递过来的活动ID参数
		 */
		protected void setup(Context context) throws IOException,InterruptedException {
			super.setup(context);
			orderIdArr = context.getConfiguration().get("orderId").split(",");
			mediaArr = context.getConfiguration().get("mediaPar").split(",");
			inventoryidArr = context.getConfiguration().get("inventoryidPar").split(",");
			collectArr = context.getConfiguration().get("collectPar");
		}
		public void map (LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
			String [] arr = value.toString().split("\t");
			int arrLength = arr.length;
			if(arrLength>8){
				int orderIdArrLen  = orderIdArr.length;
				boolean status = false;
				for(int i=0;i<orderIdArrLen ;i++){
					if(orderIdArr[i].equals(arr[8])){//广告活动
						status = true;//找到广告活动才执行下一个判断
						break;
					}
				}
				if(status ){
					String orderid = arr[8];//活动id
					String siteid = arr[3];//媒体id
					String inventory = arr[1];//广告位id
					Set<String> siteSet = new HashSet<String>();//媒体集合  去重及用contains方法 而不是用数组长度
					for(String s : mediaArr){
						siteSet.add(s);
					}
					Set<String> inventorySet = new HashSet<String>();//广告位集合
					for(String s : inventoryidArr){
						inventorySet.add(s);
					}
					if(siteSet.contains(siteid)){//判断是否在指定媒体中
						if(inventorySet.contains(inventory)){//判断是否在指定广告位中
							String cookie = arr[5];
							if("".equals(cookie)){
								cookie = "NULL";
							}
							if("0".equals(collectArr.trim())){//非汇总
								kt.set(orderid+"\t" + siteid +"\t" + inventory +"\t"+cookie);//活动id 媒体id 广告位id  cookieid
								context.write(kt, vt);
							}else if("1".equals(collectArr.trim())){//汇总（按广告位）
								//需要查表
								String inventoryName = DBJDBCUtil.getValue(inventory);
								kt.set(orderid + "\t" + inventoryName + "\t" + cookie);//活动id 广告位id  cookieid
								context.write(kt, vt);
							}
						}
					}
				}
			}
		}
	}
	/**
	 *@function 分析指定活动下媒体广告位频次PV UV数据 REDUCE阶段
	 *@author zhou yuanlong
	 *@date 第一次修改 2013-01-30
	 *修改人 zhou.yuanlong
	 *@date 第二次修改 2013-07-03
	 *修改人 zhou.yuanlong
	 */
	public static class AnalyInventoryFrequencyReduce extends Reducer<Text, IntWritable ,Text , IntWritable >{
		
		private Text kt = new Text();
		private IntWritable vt = new IntWritable();
		
		public void reduce (Text key , Iterable<IntWritable> values ,Context context)throws IOException , InterruptedException{
			String [] keyArr = key.toString().split("\t");
			String inventoryName = null;
			if(keyArr.length == 4){//非汇总
				kt.set(keyArr[0] + "\t" + keyArr[1] + "\t" + keyArr[2]);
			}else if(keyArr.length == 3){//汇总
				/**TODO需要查表*/
//				inventoryName = DBJDBCUtil.getValue(keyArr[1]);
				inventoryName = keyArr[1];
				StringBuffer sb = new StringBuffer();
				sb.append(keyArr[0]).append("\t")
					.append(inventoryName);
				inventoryName = null;
				kt.set(sb.toString());
			}
			int sum = 0;
			for(IntWritable value : values){
				sum+=value.get();
			}
			vt.set(sum);
			context.write(kt, vt);
		}
	}
	
	
	/**
	 *@function 计算指定活动下媒体广告位频次PV UV数据 MAP阶段
	 *@output  orderid (非汇总包含siteid) inventory pv(cookie) 
	 *修改人 zhou.yuanlong
	 *@date 第一次修改 2013-01-30
	 *@author zhou yuanlong
	 */
	public static class SumInventoryFrequencyMap extends Mapper<LongWritable, Text, Text, IntWritable>{
		private Text kt = new Text();
		private IntWritable vt = new IntWritable();
		private int frequency ;
		
		protected void setup(Context context) throws IOException,InterruptedException {
			super.setup(context);
			frequency = Integer.valueOf(context.getConfiguration().get("frequency"));
		}
		public void map (LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
			String [] arr = value.toString().split("\t");
			if(arr.length == 4){//非汇总
				int i = Integer.valueOf(arr[3]);
				kt.set(arr[0]+"\t"+arr[1]+"\t"+arr[2]+"\t"+(i>frequency?frequency:i));//最高频次	
				vt.set(i);
				context.write(kt, vt);
			}else if(arr.length == 3){//汇总
				int i = Integer.valueOf(arr[2]);
				kt.set(arr[0]+"\t"+arr[1]+"\t"+(i>frequency?frequency:i));//最高频次
				vt.set(i);
				context.write(kt, vt);
			}
		}
	}
	
	/**
	 *@function 计算指定活动下媒体广告位频次PV UV数据 REDUCE阶段不是啊 
	 *@output  orderid frequency pv(cookie) uv(cookie)
	 *@author zhou yuanlong
	 *修改人 zhou.yuanlong
	 *@date 第一次修改 2013-01-30
	 */
	public static  class SumInventoryFrequencyReduce extends Reducer<Text, IntWritable ,Text , Text >{
		
		private Text vt = new Text();
		private String impOrClk ;
		
		protected void setup(Context context) throws IOException,InterruptedException {
			super.setup(context);
			impOrClk = context.getConfiguration().get("impOrClk");
		}
		public void reduce (Text key , Iterable<IntWritable> values ,Context context)throws IOException , InterruptedException{
			int sum = 0;
			int uv = 0;
			for(IntWritable value : values){
				sum += value.get();
				uv++;
			}
			if("pv,uv".equals(impOrClk) || "clk,uc".equals(impOrClk) ){
//				vt.set(sum+"\t"+uv);
				vt.set(sum+"\t"+uv+"\t"+impOrClk );//TODO zhou.yuanlong
			}else if("pv".equals(impOrClk) || "clk".equals(impOrClk)){
				vt.set(String.valueOf(sum));
			}else if("uv".equals(impOrClk) || "uc".equals(impOrClk)){
				vt.set(String.valueOf(uv));
			}
			context.write(key, vt);
		}
	}
	
	/**
	 * 
	* @ClassName: AnalySiteCrowdMap
	* @Description: TODO(网站人群分析的MAP分析第一步   主要分析PV UV VISIT UIP AVGDURATION平均停留时间     RV回访数  BOUNCED跳出数)
	* @author: ping.jie
	* @date: 2012-11-27   上午11:54:22 
	* @revision          $Id
	 */
	public static class AnalySiteCrowdMap extends Mapper<LongWritable, Text, Text, Text> {
		
		private Text outkey = new Text();
		private Text outValue = new Text();
		private List<String> SiteIDList = new ArrayList<String>();
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
			String[] arrSiteID = context.getConfiguration().get("siteId").split(","); 
			SiteIDList = Arrays.asList(arrSiteID);
		}


		public void map(LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
			String[] rows = value.toString().split("\t");
			if(rows.length >= 22) {
				String cookieId = rows[2].trim();
				String siteId = "".equals(rows[8].trim())?"0":rows[8];//网站ID
				String regex = "^[0-9]+$";
				Matcher a = Pattern.compile(regex).matcher(siteId);
				if(SiteIDList.contains(siteId)) {
					String goalStr = rows[9].trim();
					int pv = 0;
					if(a.matches()){
						if("".equals(goalStr) || goalStr == null) {
							pv = 1;
						}
						String createTime = rows[7].trim();
						String url = rows[10].trim();
						outkey.set(cookieId + "\t" + siteId);
						outValue.set(createTime + "\t" + pv + "\t" + ("".equals(url) ? "NULL" : url));
						context.write(outkey, outValue);
					}
					outkey.set(siteId + "allUV");
					outValue.set(cookieId);
					context.write(outkey, outValue);
				}
			}
		}
	}
	public static class AnalyseSiteCrowdReduce extends Reducer<Text, Text, Text, Text> {
		private Text outKey = new Text();
		private Text OutValue = new Text();
		public void reduce(Text key , Iterable<Text> values ,Context context)throws IOException , InterruptedException{
			String arrKey[] = key.toString().split("\t");
			if(arrKey.length == 2) {
				Map<String,String> datas = new HashMap<String,String>();
				
				for(Text value : values) {
					String[] time_ip = value.toString().trim().split("\t");
					if(time_ip.length == 3) {
						if(datas.containsKey(time_ip[0])) {
							String[] datainfo = datas.get(time_ip[0]).split("\t");
							int lastPv = Integer.parseInt(datainfo[0]);
							datas.put(time_ip[0],(Integer.parseInt(time_ip[1])+lastPv) + "\t" + time_ip[2]);
							
						}else{
							datas.put(time_ip[0], time_ip[1] + "\t" + time_ip[2] );
						}
					}
				}
				TreeMap<String,String> sortDates = new TreeMap<String,String>(datas);
				String result = CommonUtil.anylaseSiteData(sortDates);
				
				OutValue.set(result + "\t" + 1);// visit pv bounced duration totalVisitDepth uv
				
				
				Set<String> crowdIds = RedisUtil.hKeys(arrKey[0]);
				for(String crowd : crowdIds) {
					outKey.set(crowd + "\t" + arrKey[1]);
					context.write(outKey, OutValue);
				}
				if(crowdIds.size() > 0) {
					outKey.set(arrKey[1] + "matchUV");
					OutValue.set("1");
					context.write(outKey, OutValue);
					
				}
			}else{//计算总UV
				Set<String> cookies = new HashSet<String>();
				for(Text value : values) {
					cookies.add(value.toString().trim());
				}
				OutValue.set(cookies.size()+"");
				context.write(key, OutValue);
			}
			
		}
	}
	
	/**
	 * 
	* @ClassName: SumSiteAnalyResultMap
	* @Description: TODO(网站人群分析结果按 siteID 人群 进行整合)
	* @author: ping.jie
	* @date: $date
	* @revision          $Id
	 */
	public static class sumSiteCrowdMap extends Mapper<LongWritable, Text, Text, Text> {
		
		private Text outKey = new Text();
		private Text outValue = new Text();
		public void map(LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
			String rows[] = value.toString().split("\t");
			if(rows.length == 8) {
				// crowdid siteid visit pv bounced duration totalVisitDepth uv
				String crowdId = rows[0].trim();
				String siteId = rows[1].trim();
				StringBuffer outSb = new StringBuffer();
				outSb.append(rows[2].trim()).append("\t").
				append(rows[3].trim()).append("\t").
				append(rows[4].trim()).append("\t").
				append(rows[5].trim()).append("\t").
				append(rows[6].trim()).append("\t").
				append(rows[7].trim());
				outKey.set(siteId + "\t" + crowdId);
				outValue.set(outSb.toString());
				context.write(outKey, outValue);
			}else if(rows.length == 2) {
				outKey.set(rows[0]);
				outValue.set(rows[1]);
				context.write(outKey, outValue);
			}
		}
	}
	
	public static class sumSiteCrowdReduce extends Reducer<Text, Text, Text, Text> {
		
		private Text outValue = new Text();
		private DecimalFormat df = new DecimalFormat("0.00%");
		private String metrics;
		
		protected void setup(Context context) throws IOException,
		InterruptedException {
			super.setup(context);
			metrics  = context.getConfiguration().get("metrics");
		}
		public void reduce(Text key , Iterable<Text> values ,Context context)throws IOException , InterruptedException{
			String arrKey[] = key.toString().split("\t");
			if(arrKey.length == 1) {
				int count = 0;
				for(Text value : values) {
					count += Integer.parseInt(value.toString().trim());
				}
				outValue.set(count+"");
				context.write(key, outValue);
			}else{
				//visit pv bounced duration totalVisitDepth uv
				int visit = 0;
				int pv = 0;
				int uv = 0;
				float bounced = 0;
				float duration = 0;
				float totalVisitDepth = 0;
				float bounce  = 0;
				float avgDutation = 0;
				for(Text value : values) {
					String[] results = value.toString().split("\t");
					visit += Integer.parseInt(results[0].trim());
					pv += Integer.parseInt(results[1].trim());
					bounced += Integer.parseInt(results[2].trim());
					duration += Integer.parseInt(results[3].trim());
					totalVisitDepth += Integer.parseInt(results[4].trim());
					uv += Integer.parseInt(results[5].trim());
				}
				avgDutation = duration/visit; //平均停留时间
				bounce = bounced/visit;//跳出率
				float avgVisitURL = totalVisitDepth/visit;//唯一平均访问页数     总的唯一访问深度/visit
				
				//筛选出需要的值
				String[] arrMetrics = metrics.split(",");
				StringBuffer outSB = new StringBuffer();
				for(String metric : arrMetrics) {
					if("pv".equalsIgnoreCase(metric)) {
						outSB.append(pv).append("\t");
					}else if("uv".equalsIgnoreCase(metric)){
						outSB.append(uv).append("\t");
					}else if("visit".equalsIgnoreCase(metric)){
						outSB.append(visit).append("\t");
					}else if("avgduration".equalsIgnoreCase(metric)){
						outSB.append(formatSecond((float)(Math.round(avgDutation*100))/100)).append("\t");
					}else if("bounce".equalsIgnoreCase(metric)){
						outSB.append(df.format((double)(Math.round(bounce*10000))/10000)).append("\t");
					}else if("visitdepth".equalsIgnoreCase(metric)){
						outSB.append((float)(Math.round(avgVisitURL*10))/10).append("\t");
					}
				}
				String outStr = outSB.toString();
				outValue.set(outStr.substring(0, outStr.length()-1));
				context.write(key, outValue);
				//siteid visit pv uv bounce avgduration uip rv avgvisitdepth
			}
		}
		
		/**
		 * 将double格式的秒 转换成 HH：MM :ss 格式的形式
		 * @param allSecond
		 * @return
		 */
		
		private static String formatSecond(double allSecond) {
			int hour = 0;
			int min = (int) (allSecond / 60);
			int second = (int) (allSecond % 60);
			if(min >= 60) {
				hour = min / 60;
				min = min % 60;
			}
			StringBuffer strTime = new StringBuffer();
			if(hour == 0) {
				strTime.append("00:");
			}else if(hour < 10){
				strTime.append("0").append(hour).append(":");
			}else{
				strTime.append(hour).append(":");
			}
			
			if(min == 0) {
				strTime.append("00:");
			}else if(min < 10){
				strTime.append("0").append(min).append(":");
			}else{
				strTime.append(min).append(":");
			}
			
			if(second == 0) {
				strTime.append("00");
			}else if(second < 10){
				strTime.append("0").append(second);
			}else{
				strTime.append(second);
			}
			return strTime.toString();
		}
		
	}
	
	/** 
	 * @function 计算广告位频次数据 Map 阶段
	 * @output key :非汇总：活动id,媒体id,广告位,频次；汇总： 活动id,广告位name,频次
	 *       value : pv/clk , uv/uc , "imp"/"clk"
	 *
	 */
	public static class CombineInventoryFrequencyPvUvClkUcMap extends Mapper<LongWritable, Text, Text, Text>{
		private Text kt = new Text();
		private Text vt = new Text();
		
		public void map (LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
			String [] arr = value.toString().split("\t");
			if(arr.length == 7){//非汇总
				kt.set(arr[0]+"\t"+arr[1]+"\t"+arr[2]+"\t"+arr[3]);
				vt.set(arr[4]+"\t"+arr[5]+"\t"+arr[6]);
			}else if(arr.length == 6){//汇总
				kt.set(arr[0]+"\t"+arr[1]+"\t"+arr[2]);
				vt.set(arr[3]+"\t"+arr[4]+"\t"+arr[5]);
			}
			context.write(kt, vt);
		}
	}
	
	/**
	 * @function 合并广告位频次的展现点击结果文件
	 * @output  key:非汇总：活动id,媒体id,广告位,频次；汇总： 活动id,广告位name,频次
	 *         value: pv/clk , uv/uc
	 *
	 */
	public static class CombineInventoryFrequencyPvUvClkUcReduce  extends Reducer<Text, Text ,Text , Text>{
		private Text vt = new Text();
		private String metrics = null;
		protected void setup(Context context) throws IOException,
		InterruptedException {
			super.setup(context);
			metrics  = context.getConfiguration().get("metrics");
		}
		
		public void reduce (Text key , Iterable<Text> values ,Context context)throws IOException , InterruptedException{
			String clkValue =0+"\t"+0;
			String impValue =0+"\t"+0;
			for(Text value : values){
				String arr[] = value.toString().split("\t");
				if("pv,uv".equals(arr[2])){
					impValue = arr[0]+"\t"+arr[1];
				}else if("clk,uc".equals(arr[2])){
					clkValue = arr[0]+"\t"+arr[1];
				}
			}
			String outputValue = CommonUtil.beSureOutputValue(metrics, impValue, clkValue);
			vt.set(outputValue);
			context.write(key, vt);
		}
	}
	
	/** 
	 * @function 计算活动频次数据 Map 阶段
	 * @output key :活动id,频次
	 *       value : pv/clk , uv/uc , "imp"/"clk"
	 *
	 */
	public static class CombineOrderFrequencyPvUvClkUcMap extends Mapper<LongWritable, Text, Text, Text>{
		private Text kt = new Text();
		private Text vt = new Text();
		
		public void map (LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
			String [] arr = value.toString().split("\t");
			if(arr.length == 5){
				kt.set(arr[0]+"\t"+arr[1]);
				vt.set(arr[2]+"\t"+arr[3]+"\t"+arr[4]);
			}
			context.write(kt, vt);
		}
	}
	
	/**
	 * @function 合并活动频次的展现点击结果文件
	 * @output  key:活动id,频次
	 *         value: pv/clk , uv/uc
	 *
	 */
	public static class CombineOrderFrequencyPvUvClkUcReduce  extends Reducer<Text, Text ,Text , Text>{
		private Text vt = new Text();
		private String metrics = null;
		protected void setup(Context context) throws IOException,
		InterruptedException {
			super.setup(context);
			metrics  = context.getConfiguration().get("metrics");
		}
		
		public void reduce (Text key , Iterable<Text> values ,Context context)throws IOException , InterruptedException{
			String clkValue =0+"\t"+0;
			String impValue =0+"\t"+0;
			for(Text value : values){
				String arr[] = value.toString().split("\t");
				if("pv,uv".equals(arr[2])){
					impValue = arr[0]+"\t"+arr[1];
				}else if("clk,uc".equals(arr[2])){
					clkValue = arr[0]+"\t"+arr[1];
				}
			}
			String outputValue = CommonUtil.beSureOutputValue(metrics, impValue, clkValue);
			vt.set(outputValue);
			context.write(key, vt);
		}
	}
	
	
	/** 
	 * @function 计算媒体频次数据 Map 阶段
	 * @output key :活动id,媒体id,频次
	 *       value : pv/clk , uv/uc , "imp"/"clk"
	 *
	 */
	public static class CombineOrderSiteFrequencyPvUvClkUcMap extends Mapper<LongWritable, Text, Text, Text>{
		private Text kt = new Text();
		private Text vt = new Text();
		
		public void map (LongWritable key , Text value , Context context)throws IOException ,InterruptedException{
			String [] arr = value.toString().split("\t");
			if(arr.length == 6){//汇总
				kt.set(arr[0]+"\t"+arr[1]+"\t"+arr[2]);
				vt.set(arr[3]+"\t"+arr[4]+"\t"+arr[5]);
			}
			context.write(kt, vt);
		}
	}
	
	/**
	 * @function 合并媒体频次的展现点击结果文件
	 * @output  key:活动id,媒体id,频次
	 *         value: pv/clk , uv/uc
	 *
	 */
	public static class CombineOrderSiteFrequencyPvUvClkUcReduce  extends Reducer<Text, Text ,Text , Text>{
		private Text vt = new Text();
		private String metrics = null;
		protected void setup(Context context) throws IOException,
		InterruptedException {
			super.setup(context);
			metrics  = context.getConfiguration().get("metrics");
		}
		
		public void reduce (Text key , Iterable<Text> values ,Context context)throws IOException , InterruptedException{
			String clkValue =0+"\t"+0;
			String impValue =0+"\t"+0;
			for(Text value : values){
				String arr[] = value.toString().split("\t");
				if("pv,uv".equals(arr[2])){
					impValue = arr[0]+"\t"+arr[1];
				}else if("clk,uc".equals(arr[2])){
					clkValue = arr[0]+"\t"+arr[1];
				}
			}
			String outputValue = CommonUtil.beSureOutputValue(metrics, impValue, clkValue);
			vt.set(outputValue);
			context.write(key, vt);
		}
	}
	
	/*public static class InventoryFrequencyPartitioner extends Partitioner<Text, Text> {  
		  
//		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) {

			// return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
			String[] keyArr = key.toString().split("\t");
			StringBuffer sb = new StringBuffer();
			for (int i = 0; i < keyArr.length-1; i++) {
				sb.append(keyArr[i]);
			}
			return (sb.toString().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
			// if()
			// return (numReduceTasks-1) % numReduceTasks;
		} 
		  
		}*/
	
	/**
	 * 自己定义的key类应该实现WritableComparable接口
	 * @author zhou.yuanlong
	 * @date 2013-08-21
	 */
    public static class TextIntPair implements WritableComparable<TextIntPair>
    {
        String first;
        int second;
        public void set(String left, int right)
        {
            first = left;
            second = right;
        }
        public String getFirst()
        {
            return first;
        }
        public int getSecond()
        {
            return second;
        }
        //反序列化，从流中的二进制转换成TextIntPair
        public void readFields(DataInput in) throws IOException
        {
            first = in.readUTF();
            second = in.readInt();
        }
        //序列化，将TextIntPair转化成使用流传送的二进制
        public void write(DataOutput out) throws IOException
        {
            out.writeUTF(first);
            out.writeInt(second);
        }
        //key的比较
        public int compareTo(TextIntPair o)
        {
            if (!first.equals(o.first))
            {
                return first.hashCode() < o.first.hashCode() ? -1 : 1;
            }
            else if (second != o.second)
            {
                return second < o.second ? -1 : 1;
            }
            else
            {
                return 0;
            }
        }

        //新定义类应该重写的两个方法
        @Override
        public int hashCode()
        {
            return first.hashCode() + second;//TODO why 157?
        }
        /**
         * 这个方法的参数是Object,既可以比较int类型的,也可以比较TextIntPair类型的
         */
        @Override
        public boolean equals(Object right)
        {
            if (right == null)
                return false;
            if (this == right)
                return true;
            if (right instanceof TextIntPair)//TODO 比较这做什么呢
            {
                TextIntPair r = (TextIntPair) right;
                return r.first.equals(first) && r.second == second;
            }
            else
            {
                return false;
            }
        }
    }
    /**
      * 分区函数类。根据first确定Partition。
      * 别忘了分片是value也要是Text
      */
    public static class FirstPartitioner extends Partitioner<TextIntPair, Text>
    {
        @Override
        public int getPartition(TextIntPair key, Text value,int numPartitions)
        {
            return Math.abs(key.getFirst().hashCode()) % numPartitions;//用绝对值取模，因为hashcode可能是负数
        }
    }

    /**
     * 分组函数类。只要first相同就属于同一个组。
     */
    public static class GroupingComparator extends WritableComparator
    {
        protected GroupingComparator()
        {
            super(TextIntPair.class, true);
        }
        @Override
        public int compare(WritableComparable w1, WritableComparable w2)
        {
            TextIntPair ip1 = (TextIntPair) w1;
            TextIntPair ip2 = (TextIntPair) w2;
            String l = ip1.getFirst();//TODO hashcode不行
            String r = ip2.getFirst();
            return l.hashCode() == r.hashCode() ? 0 : (l.hashCode() < r.hashCode() ? -1 : 1);//若TextIntPair类型第一个数的first(int型)和第二个数的first相等则返回0，若小于则返回-1，否则（即大于）则返回1
        }
    }
    
    public static class SecondarySortMap extends Mapper<LongWritable, Text, TextIntPair, Text>
    {
        private final TextIntPair intkey = new TextIntPair();
        private final Text textvalue = new Text();
        private int frequencyIndex = 0;
        private int inputLineLength = 0;
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			frequencyIndex  = context.getConfiguration().getInt("frequencyIndex", 0);
			inputLineLength  = context.getConfiguration().getInt("inputLineLength", 0);
		}
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
        	if(frequencyIndex!=0 && inputLineLength!=0){
	            String[] line = value.toString().split("\t");
	            //key
	            StringBuffer left = new StringBuffer();
	            for(int i=0; i<frequencyIndex-1; i++){//添加前frequencyIndex-1位，带制表符
	            	left.append(line[i]).append("\t");
	            }
	            left.append(line[frequencyIndex-1]);//添加第frequencyIndex位，不带制表符
	            int right = Integer.parseInt(line[frequencyIndex]);// 频次的数
	            
	            //value
	            StringBuffer vt = new StringBuffer();
	            for(int i=frequencyIndex; i<inputLineLength-1; i++){//从frequencyIndex（频次所在位）位开始添加，直到倒数第二位，带制表符。注意需要把频次也添加进去
	            	vt.append(line[i]).append("\t");
	            }
	            vt.append(line[inputLineLength-1]);//添加最后一位inputLineLength-1，带制表符
	            
				intkey.set(left.toString(), right);
				textvalue.set(vt.toString());
				context.write(intkey, textvalue);
        	}
        }
    }
    
    public static class SecondarySortReduce extends Reducer<TextIntPair, Text, Text, Text>
    {
        private final Text left = new Text();
        
        public void reduce(TextIntPair key, Iterable<Text> values,Context context) throws IOException, InterruptedException
        {
            left.set(key.getFirst());
            for (Text val : values)
            {
                context.write(left, val);
            }
        }
    }
    
    
    
    
    /************************************************************************************************************
     * TODO 曝光频次覆盖报告
     * @author wang meng(uni)
     * @date 2013-10-24 14:13
     */
    
    /**
	 * @function 频次报告Map阶段
	 * @output cookieid 1 
	 */
	public static class AnalyseUniFrequencyMap extends Mapper<LongWritable, Text, Text, IntWritable>{
		private Text outKey = new Text();
		private IntWritable outValue = new IntWritable(1);
		private String beginDate ;
		private String endDate  ;
		private String orderid ;
		private List<String> orderidList = new ArrayList<String>();
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
			beginDate = context.getConfiguration().get("beginDate");
			endDate = context.getConfiguration().get("endDate");
			orderid = context.getConfiguration().get("orderid");
			String[] arrIds = orderid.split(",");
			for(String orderid : arrIds){
				orderidList.add(orderid);
			}
			
		}
		public void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String[] rows = value.toString().split("\t");
			if(rows.length>9 && !"".equals(rows[9]) && orderidList.contains(rows[8])){
				String createTime = rows[9].trim().substring(0, 10);
				String[] arrDate = CommonUtil.arrStartAndEndTime(createTime,endDate);
				int i = 0 ;
				for(String date : arrDate) {
					outKey.set((beginDate+"_"+date)+"\t"+rows[5]);//date cookie
					context.write(outKey, outValue);//date cookie 1
				}
			}
		}
	}
	/**
	 * @function 频次报告Reduce阶段
	 * @output date cookieid 频次 
	 */
	public static class AnalyseUniFrequencyReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable outValue = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values,Context context)
				throws IOException, InterruptedException {
			int frequency = 0 ;
			for(IntWritable value : values){
				frequency += value.get();
			}
			outValue.set(frequency);
			context.write(key, outValue);//date cookie PC
		}
	}
	/**
	 * @function 频次报告Map2阶段
	 * @output frequency  1(因有时间控制，故cookie一样也不是同频次)
	 */
	public static class SumUniFrequencyMap extends Mapper<LongWritable, Text, Text, IntWritable>{
		private Text outKey = new Text();
		private IntWritable outValue = new IntWritable(1);
		public void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String[] arr = value.toString().split("\t");
				outKey.set(arr[0]+"\t"+arr[2]);
				context.write(outKey, outValue);//date PC 1
		}
	}
	/**
	 * @function 频次报告Reduce2阶段
	 * @output  frequency  PV  UV
	 */
	public static class SumUniFrequencyReduce extends Reducer<Text, IntWritable, Text, Text>{
		private Text outKey = new Text();
		private Text outValue = new Text();
		public void reduce(Text key, Iterable<IntWritable> values,Context context)
				throws IOException, InterruptedException {
			int UV = 0 ;
			int frequency = 0 ;
			int PV = 0 ;
			for(IntWritable value:values){
				UV += value.get();
			}
			String[] arrKey = key.toString().split("\t");
			StringBuffer sb = new StringBuffer();
			String date = arrKey[0].substring(11, 21);
			frequency += Integer.parseInt(arrKey[1]);
			PV = frequency*UV ;
			sb.append(date).append("\t").append(frequency);
			outKey.set(sb.toString());
			outValue.set(PV+"\t"+UV);
			context.write(outKey, outValue);
		}
	}
	////////////////////////////////////////////////////////////////////////////////////////////////////////////
	
	/**
	 * 分析UA
	 * @author zhou yuanlong
	 *
	 */
	public static class AnalySiteMobileDevicesUAMap extends Mapper<LongWritable, Text, Text, Text>{
//		private Text kt = new Text();
//		private Text vt = new Text();
//		private String regex = "^[0-9]+$";
		private String [] siteIdArr ;
		
		/***
		 * 获取从JobControl端传递过来的活动ID参数
		 */
		protected void setup(Context context) throws IOException,
		InterruptedException {
			super.setup(context);
			siteIdArr = context.getConfiguration().get("site").split(",");
		}
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String [] arr = value.toString().split("\t");
			if(arr.length > 8){
				int siteIdArrLen  = siteIdArr.length;
				boolean status = false;
				for(int i=0;i<siteIdArrLen ;i++){
					if(siteIdArr[i].equals(arr[8])){
						status = true;
						break;
					}
				}
				if(status){
					context.write(value, new Text(""));
				}
				
			}
			
		}
	}
	
	/**
	 * 分析UA
	 * @author zhou yuanlong
	 *
	 */
	public static class AnalySiteMobileDevicesUAReduce extends Reducer<Text, Text, Text, Text>{
		private Text outKey = new Text();
		private Text outValue = new Text();
		private String wurflPath = "/home/unihadoop/test/wurfl.xml";
		private WURFLUtil wurfl  = new WURFLUtil(wurflPath);
		
		private String flag ;
		
		protected void setup(Context context) throws IOException,InterruptedException {
			super.setup(context);
			flag = context.getConfiguration().get("flag");
		}
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			String[] keyArr = key.toString().split("\t");
			String userAgent = keyArr[21];
			for(Text value : values) {
				DeviceType deviceType = wurfl.getDeviceType(userAgent);
				StringBuffer sb = new StringBuffer();
				
				String siteCookieids = "NULL";
				String clkCookieAdIds = "NULL";
				if(keyArr.length > 23) {
					clkCookieAdIds = keyArr[23].trim();
					siteCookieids = keyArr[24].trim();
				}
				
				String uaDeviceInfo = "";
				if("mobileDevice".equals(flag)){//网站移动设备报告
					if(deviceType.MOBILE.equals(deviceType) || deviceType.TABLET.equals(deviceType)){//手机或平板
						//操作系统 OK
						String CapabilityName = WURFLCapabilityConstant.ADVERTISED_DEVICE_OS;
						String capability = wurfl.getCapability(userAgent, CapabilityName);
						String os = capability;
						if("".equals(os)){
							os = "未知操作系统";
						}
						
						//设备型号
						CapabilityName = WURFLCapabilityConstant.DEVICE_ID;
						capability = wurfl.getCapability(userAgent, CapabilityName);
						String deviceModel = capability;
						if("".equals(deviceModel) || " ".equals(deviceModel)){
							deviceModel = "未知设备型号";
						}
						
						//鼠标类型 是否触屏 OK
						CapabilityName = WURFLCapabilityConstant.IS_TOUCHSCREEN;
						capability = wurfl.getCapability(userAgent, CapabilityName);
						String touchscreen = "普通";
						if("true".equals(capability)){
							touchscreen = "触屏";
						}else{
							touchscreen = "普通";
						}
						
						uaDeviceInfo = os+"\t"+deviceModel+"\t"+touchscreen;
					}
				}else if("userDeviceType".equals(flag)){//网站用户设备类型报告
					//设备类型
					String devicesType = "其它（如机顶盒）";
					if(deviceType.PC.equals(deviceType)){
						devicesType = "PC";
					}else if(deviceType.MOBILE.equals(deviceType)){
						devicesType = "手机";
					}else if(deviceType.TABLET.equals(deviceType)){
						devicesType = "平板电脑";
					}else if(deviceType.OTHER.equals(deviceType)){
						devicesType = "其它（如机顶盒）";
					}
					
					uaDeviceInfo = devicesType;
				}
				if(!"".equals(uaDeviceInfo)){
					sb.append(keyArr[0]).append("\t")
						.append(keyArr[1]).append("\t")
						.append(keyArr[2]).append("\t")
						.append(keyArr[3]).append("\t")
						.append(keyArr[4]).append("\t")
						.append(keyArr[5]).append("\t")
						.append(keyArr[6]).append("\t")
						.append(keyArr[7]).append("\t")
						.append(keyArr[8]).append("\t")
						.append(keyArr[9]).append("\t")
						.append(keyArr[10]).append("\t")
						.append(keyArr[11]).append("\t")
						.append(keyArr[12]).append("\t")
						.append(keyArr[13]).append("\t")
						.append(keyArr[14]).append("\t")
						.append(keyArr[15]).append("\t")
						.append(keyArr[16]).append("\t")
						.append(keyArr[17]).append("\t")
						.append(keyArr[18]).append("\t")
						.append(keyArr[19]).append("\t")
						.append(keyArr[20]).append("\t")
						.append(uaDeviceInfo).append("\t")
						.append(keyArr[22]).append("\t")
						.append(clkCookieAdIds).append("\t")
						.append(siteCookieids);
					outKey.set(sb.toString());
					outValue.set("");
					context.write(outKey, outValue);
				}
			}
		}
	}
	
	/**
	 * 
	 *    
	 * 项目名称：AnalyseCenter   
	 * 类名称：AnalySiteReferral   
	 * 类描述： 分析网站来源数据	MAP阶段
	 * 创建人：yang qi   
	 * 创建时间：2012-12-3 下午02:16:35   
	 * 修改人：yang qi   
	 * 修改时间：2012-12-3 下午02:16:35   
	 * 修改备注：
	 * @output	key: Cookie , 网站ID
	 * 		value: 时间  , Referral , clkCookieIDS , siteCookieids , PV   , URL  
	 * @version    
	 *
	 */
	public static class AnalySiteMobileDevicesMap extends Mapper<LongWritable, Text, Text, Text>{

		private Text kt = new Text();
		private Text vt = new Text();
		private String regex = "^[0-9]+$";
		
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String [] arr = value.toString().split("\t");
			//过滤缺失日志
			if(arr.length>22){
				String siteId = arr[8];//网站ID
				String siteCookieids = "NULL";
				String clkCookieAdIds = "NULL";
				String ua = "NULL";
//				String ua = "设备类型";//UA
				if(arr.length > 26) {
					clkCookieAdIds = arr[25].trim();
					siteCookieids = arr[26].trim();
					ua = arr[21]+"\t"+arr[22]+"\t"+arr[23];//UA
				}else if(arr.length > 24) {
					clkCookieAdIds = arr[23].trim();
					siteCookieids = arr[24].trim();
					ua = arr[21];//UA
				}
				Matcher a = Pattern.compile(regex).matcher(siteId);
				if(a.matches()  && !"111".equals(siteId)){
					StringBuffer keyBuffer = new StringBuffer();
					StringBuffer valueBuffer = new StringBuffer();
					/*keyBuffer.append(arr[2]).append("\t")//cookie
					.append(siteId);*/
					keyBuffer.append(arr[2]).append("\t")//cookie
					.append(ua);//设备类型
					//Cookie , 网站ID
					kt.set(keyBuffer.toString());
					/*int pv = 0;
					//主链接不计PV
					if("".equals(arr[9]) || arr[9] == null) {
						pv = 1;
					}*/
					int pv = 1;
					valueBuffer.append(arr[7]).append("\t")//时间
					.append("".equals(arr[11].trim())?"NULL":arr[11].trim()).append("\t")//来源
					.append(clkCookieAdIds).append("\t")//投放来源
					.append(siteCookieids).append("\t")//回访数
					.append(pv).append("\t")//goalStr pv
					.append("".equals(arr[10].trim())?"NULL":arr[10].trim());//url
					//时间  , Referral , clkCookieIDS , siteCookieids , PV ,URL
					vt.set(valueBuffer.toString());
					context.write(kt, vt);
				}
			}
		}
	}
	
	/**
	 * 
	 *    
	 * 项目名称：AnalyseCenter   
	 * 类名称：AnalySiteReferral   
	 * 类描述： 分析网站来源数据	Reduce阶段
	 * 创建人：yang qi   
	 * 创建时间：2012-12-3 下午02:16:35   
	 * 修改人：yang qi   
	 * 修改时间：2012-12-3 下午02:16:35   
	 * 修改备注：
	 * @output	key: Cookie , 网站ID
	 * 		value: 浏览量 , 停留时间 , 来源类型 , domain , 0 , 0 , 跳出数 , 回访数 , 去重URL
	 * @version    
	 *
	 */
	public static class AnalySiteMobileDevicesReduce extends Reducer<Text, Text, Text, Text>{

		private Text vt = new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			Map<String,String> datas = new HashMap<String,String>();
			
			int rv = 0; //回访数
			boolean isFrist = true; //是否第一次进入循环
			for(Text value : values) {
				//时间  , Referral , clkCookieIDS , siteCookieids , PV ,URL
				String[] time_data = value.toString().split("\t");
				if(isFrist) {
					String siteCookieIds = time_data[3]; //记录上次访问时间的字段
					if(!"NULL".equalsIgnoreCase(siteCookieIds) && !"".equals(siteCookieIds) && siteCookieIds != null) {
						rv = 1;
						isFrist = false;
					}
				}
				if(time_data.length == 6) {
					if(datas.containsKey(time_data[0])) {
						String[] datainfo = datas.get(time_data[0]).split("\t");
						int lastPv = Integer.parseInt(datainfo[2]);
						datas.put(time_data[0], datainfo[0] + "\t" + datainfo[1] + "\t" + (Integer.parseInt(time_data[4])+lastPv) + "\t" + datainfo[3]);
					}else{
						datas.put(time_data[0], time_data[1] + "\t" + time_data[2] + "\t" + time_data[4] + "\t" + time_data[5] );
					}
				}
			}
			TreeMap<String,String> sortDates = new TreeMap<String,String>(datas);
			 
			 Map<String,StandardTarget> refer_date = CommonUtil.anylaseSiteDataAndRefer(sortDates);//
			 for(String referInfo : refer_date.keySet()) {
				 int visitRV = 0;
				 StandardTarget st = refer_date.get(referInfo);
				 if(rv == 1) {
					 visitRV = st.getVisit();
				 }
				 StringBuffer outsb = new StringBuffer();
				 outsb.append(st.getPv()).append("\t")
				 .append(st.getDarution()).append("\t")
				 .append(referInfo).append("\t")
				 .append(st.getBounce()).append("\t")
				 .append(rv).append("\t").append(st.getUniVisitDepth()).append("\t").append(st.getVisit())
				 .append("\t").append(visitRV);
				//浏览量 , 停留时间 , 来源类型, 广告ID , 媒体ID ,活动ID , 跳出数 , 回访数 ,去重URL,visit.visitRV
				 vt.set(outsb.toString());
				 context.write(key, vt);
			 }
			
		}
	}
	
	/**
	 * 
	 *    
	 * 项目名称：AnalyseCenter   
	 * 类名称：AnalySiteReferral   
	 * 类描述： 计算网站来源数据	MAP阶段
	 * 创建人：yang qi   
	 * 创建时间：2012-12-3 下午02:16:35   
	 * 修改人：yang qi   
	 * 修改时间：2012-12-3 下午02:16:35   
	 * 修改备注：
	 * @output	key: 网站ID , 来源类型	, 广告ID , 媒体ID ,	活动ID
	 * 		value: Cookie , 浏览量 , 停留时间 , 跳出数 , 回访数 ,去重URL
	 * @version    
	 *
	 */
	public static class SumSiteMobileDevicesMap extends Mapper<LongWritable, Text, Text, Text>{
		
		private Text kt = new Text();
		private Text vt = new Text();
		
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			//cookie,siteid,浏览量 , 停留时间 , 来源类型, 广告ID , 媒体ID ,活动ID , 跳出数 , 回访数 ,去重URL,visit
			String [] arr = value.toString().split("\t");
			if(arr.length == 13) {
				StringBuffer keyBuffer = new StringBuffer();
				StringBuffer valueBuffer = new StringBuffer();
				keyBuffer.append(arr[1])
				/*.append("\t")
				.append(arr[4]).append("\t")
				.append(arr[5]).append("\t")
				.append(arr[6]).append("\t")
				.append(arr[7])*/;
				//网站ID , 来源类型	, 广告ID , 媒体ID ,	活动ID
				kt.set(keyBuffer.toString());
				
				valueBuffer.append(arr[0]).append("\t")//cookie
				.append(arr[2]).append("\t")//浏览量
				.append(arr[3]).append("\t")//停留时间
				.append(arr[8]).append("\t")//跳出数
				.append(arr[9]).append("\t")//回访次数
				.append(arr[10]).append("\t")//去重URL
				.append(arr[11]).append("\t")//访问数
				.append(arr[12]);//回访人数
				//Cookie , 浏览量 , 停留时间 , 跳出数 , 回访数  , 去重URL,visit
				vt.set(valueBuffer.toString());
				context.write(kt, vt);
			}else if(arr.length == 15) {
				StringBuffer keyBuffer = new StringBuffer();
				StringBuffer valueBuffer = new StringBuffer();
				keyBuffer
				.append(arr[1]).append("\t")
				.append(arr[2]).append("\t")
				.append(arr[3])
				/*.append("\t")
				.append(arr[6]).append("\t")
				.append(arr[7]).append("\t")
				.append(arr[8]).append("\t")
				.append(arr[9])*/;
				//网站ID , 来源类型	, 广告ID , 媒体ID ,	活动ID
				kt.set(keyBuffer.toString());
				
				valueBuffer.append(arr[0]).append("\t")//cookie
				.append(arr[4]).append("\t")//浏览量
				.append(arr[5]).append("\t")//停留时间
				.append(arr[10]).append("\t")//跳出数
				.append(arr[11]).append("\t")//回访次数
				.append(arr[12]).append("\t")//去重URL
				.append(arr[13]).append("\t")//访问数
				.append(arr[14]);//回访人数
				//Cookie , 浏览量 , 停留时间 , 跳出数 , 回访数  , 去重URL,visit
				vt.set(valueBuffer.toString());
				context.write(kt, vt);
			}
		}
	}
	
	/**
	 * 
	 *    
	 * 项目名称：AnalyseCenter   
	 * 类名称：AnalySiteReferral   
	 * 类描述： 计算网站来源数据	Reduce阶段
	 * 创建人：yang qi   
	 * 创建时间：2012-12-3 下午02:16:35   
	 * 修改人：yang qi   
	 * 修改时间：2012-12-3 下午02:16:35   
	 * 修改备注：
	 * @output	key: 网站ID , 来源类型	, 广告ID , 媒体ID ,	活动ID
	 * 		value: 浏览量 , UV ,访问数, 平均停留时间, 跳出数 , 回访数 , 平均访问深度
	 * @version    
	 *
	 */
	public static class SumSiteMobileDevicesReduce extends Reducer<Text, Text, Text, Text>{
		
		private Text vt = new Text();
		private NumberFormat nf = NumberFormat.getPercentInstance();
		
		
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			nf.setMaximumFractionDigits(2);//小数点后面2位
			Set<String> cookieSet = new HashSet<String>();
			int pv =0 ;
			int duration = 0;
			int visit = 0;
			int rv = 0;
			int bounced = 0;
			int disUrl = 0;
			int visitRV = 0;
			for(Text value : values){
				//Cookie , 浏览量 , 停留时间 , 跳出数 , 回访数  , 去重URL,visit
				String [] valueArr = value.toString().split("\t");
				cookieSet.add(valueArr[0]);
				pv += Integer.parseInt(valueArr[1]);
				duration += Integer.parseInt(valueArr[2]);
				bounced += Integer.parseInt(valueArr[3]);
				rv+= Integer.parseInt(valueArr[4]);
				disUrl += Integer.parseInt(valueArr[5]);
				visit += Integer.parseInt(valueArr[6]) ;
				visitRV += Integer.parseInt(valueArr[7]) ;
			}
			BigDecimal big = new BigDecimal(Double.valueOf(duration)/visit);
			double avgDuration = big.divide(new BigDecimal(1), 2, BigDecimal.ROUND_HALF_UP).doubleValue();//平均停留时间
			double avgPageDepth = (new BigDecimal(Double.valueOf(disUrl)/visit)).divide(new BigDecimal(1), 1, BigDecimal.ROUND_HALF_UP).doubleValue();//平均访问深度
			String accessPageNumber = String.format("%.1f",Double.valueOf(pv) / visit);;//每次访问页数
			String bouncedRate = nf.format(Double.valueOf(bounced)/visit);//跳出率
			String avgDurationFormat = formatNumberToHourMinuteSecond(avgDuration);//格式化时间
			
			StringBuffer valueBuffer = new StringBuffer();
			/*valueBuffer.append(pv).append("\t")//浏览量
			.append(cookieSet.size()).append("\t")//UV
			.append(visit).append("\t")//访问数
			.append(avgDuration).append("\t")//平均停留时间
			.append(bounced).append("\t")//跳出数
			.append(rv).append("\t")//回访次数
			.append(avgPageDepth).append("\t")//平均访问深度
			.append(visitRV);//回访人数
*/			
			valueBuffer.append(pv).append("\t")//浏览量
			.append(visit).append("\t")//访问数
			.append(cookieSet.size()).append("\t")//UV
			.append(bouncedRate).append("\t")//跳出率
			.append(visitRV).append("\t")//回访人数
			.append(avgDurationFormat).append("\t")//平均停留时间
			.append(avgPageDepth).append("\t")//平均访问深度
			.append(accessPageNumber);//每次访问页数
			
			//浏览量 , UV ,访问数, 平均停留时间, 跳出数 , 回访数 , 平均访问深度
			vt.set(valueBuffer.toString());
			context.write(key, vt);
		}
		
		/**
		   * 格式化时间 把double格式的时间格式化为 hh:mm:ss格式
		   * @param dateDou
		   * @return
		   */
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
	}
}
