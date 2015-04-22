package com.uniclick.databank.mapreduce.job;



import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.uniclick.databank.mapreduce.common.CommonUtil;
import com.uniclick.databank.mapreduce.common.Constants;
import com.uniclick.databank.mapreduce.common.hadoop.BaseJob;


/**
 * 
 * @author yang qi
 * @date 2012-06-06
 * @version 1.0
 * @function Job配置
 */
public class JobControl extends BaseJob {
	
	private Configuration conf ;
	
	public JobControl(String queue) {
		conf = getConf(queue);
	}
	/**
	 * @param paths 输入文件路径
	 * @param queue 队列
	 * @param outputDir 输出路径
	 * @  
	 * @function 活动地域分析
	 */
	public  boolean AnalyOrderAreaJob(Path [] paths,String outputDir, String orderID){
		boolean isSuccessful = false;
		conf.set("orderID", orderID);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("AnalyOrderAreaJob");
			FileInputFormat.setInputPaths(job, paths );
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.AnalyOrderAreaMap.class);
			job.setReducerClass(MapReduce.AnalyOrderAreaReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	/**
	 * 
	 * @param inputDir 输出文件路径
	 * @param queue 队列
	 * @param outputDir 输出文件路径
	 * @prarm impOrClk 控制输出结果
	 * @function 活动地域分析
	 */
	public  boolean SumOrderAreaJob(String inputDir ,String outputDir,String impOrClk){
		boolean isSuccessful = false;
		conf.set("impOrClk", impOrClk);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("SumOrderAreaJob");
			FileInputFormat.setInputPaths(job,CommonUtil.listInPutDir(inputDir));
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.SumOrderAreaMap.class);
			job.setReducerClass(MapReduce.SumOrderAreaReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	/**
	 * 
	 * @param paths 输入文件路径
	 * @param queue 队列
	 * @param outputDir 输出文件路径
	 * @param impOrClk 控制输出结果
	 * @param metrics JOB输出字段
	 * @
	 * @function 活动地域分析
	 */
	public  boolean CombineOrderAreaJob(Path [] paths ,String outputDir ,String metrics){
		boolean isSuccessful = false;
		try {
			conf.set("metrics", metrics);
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("CombineOrderAreaJob");
			FileInputFormat.setInputPaths(job,paths);
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.CombineOrderAreaMap.class);
			job.setReducerClass(MapReduce.CombineOrderAreaReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	/**
	 * @param paths 输入文件路径
	 * @param queue 队列
	 * @param outputDir 输出路径
	 * @  
	 * @function 厂商
	 */
	public  boolean AnalyCompanyJob(Path [] paths  ,String outputDir ,String companyID){
		boolean isSuccessful = false;
		conf.set("companyID", companyID);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("AnalyCompanyJob");
			FileInputFormat.setInputPaths(job, paths );
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.AnalyCompanyMap.class);
			job.setReducerClass(MapReduce.AnalyCompanyReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	
	/**
	 * 
	 * @param inputDir 输出文件路径
	 * @param queue 队列
	 * @param outputDir 输出文件路径
	 * @prarm impOrClk 控制输出结果
	 * @function 厂商
	 */
	public  boolean SumCompanyJob(String inputDir ,String outputDir,String impOrClk){
		boolean isSuccessful = false;
		conf.set("impOrClk", impOrClk);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("SumCompanyJob");
			FileInputFormat.setInputPaths(job,CommonUtil.listInPutDir(inputDir));
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.SumCompanyMap.class);
			job.setReducerClass(MapReduce.SumOrderAreaReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	
	/**
	 * 
	 * @param paths 输入文件路径
	 * @param queue 队列
	 * @param outputDir 输出文件路径
	 * @param impOrClk 控制输出结果
	 * @param metrics 输出字段
	 * @
	 * @function 厂商
	 */
	public  boolean CombineCompanyJob(Path [] paths ,String outputDir ,String metrics){
		boolean isSuccessful = false;
		conf.set("metrics", metrics);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("CombineCompanyJob");
			FileInputFormat.setInputPaths(job,paths);
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.CombineCompanyMap.class);
			job.setReducerClass(MapReduce.CombineOrderAreaReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	

	/**
	 * 
	 * @param paths JOB输入文件路径
	 * @param queue JOB队列
	 * @param outputDir JOB输出路径
	 * @param orderID 活动ID
	 * @
	 * @function 分析指定活动下的广告位数据
	 */
	public  boolean AnalyInventoryJob(Path [] paths  ,String outputDir  ,String orderID){
		boolean isSuccessful = false;
		conf.set("orderID", orderID);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("AnalyInventoryJob");
			FileInputFormat.setInputPaths(job, paths );
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.AnalyInventoryMap.class);
			job.setReducerClass(MapReduce.AnalyInventoryReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	
	/**
	 * 
	 * @param inputDir 输出文件路径
	 * @param queue 队列
	 * @param outputDir 输出文件路径
	 * @prarm impOrClk 控制输出结果
	 * @function 广告位
	 */
	public  boolean SumInventoryJob(String inputDir ,String outputDir,String impOrClk){
		boolean isSuccessful = false;
		conf.set("impOrClk", impOrClk);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("SumInventoryJob");
			FileInputFormat.setInputPaths(job,CommonUtil.listInPutDir(inputDir));
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.SumInventoryMap.class);
			job.setReducerClass(MapReduce.SumOrderAreaReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	
	/**
	 * 
	 * @param paths 输入文件路径
	 * @param queue 队列
	 * @param outputDir 输出文件路径
	 * @param impOrClk 控制输出结果
	 * @param metrics JOB输出字段
	 * @
	 * @function 广告位
	 */
	public  boolean CombineInventoryJob(Path [] paths ,String outputDir ,String metrics){
		boolean isSuccessful = false;
		conf.set("metrics", metrics);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("CombineInventoryJob");
			FileInputFormat.setInputPaths(job,paths);
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.CombineInventoryMap.class);
			job.setReducerClass(MapReduce.CombineOrderAreaReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	

	/**
	 * 
	 * @param paths JOB输入文件路径
	 * @param queue JOB所在队列
	 * @param outputDir JOB输出文件路径
	 * @param orderID 活动ID
	 * @
	 * @function 分析媒体定制报表
	 */
	public  boolean AnalyMediaJob(Path [] paths  ,String outputDir , String orderID){
		boolean isSuccessful = false;
		conf.set("orderID", orderID);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("AnalyMediaJob");
			FileInputFormat.setInputPaths(job, paths );
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.AnalyMediaMap.class);
			job.setReducerClass(MapReduce.AnalyMediaReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	
	/**
	 * 
	 * @param inputDir 输出文件路径
	 * @param queue 队列
	 * @param outputDir 输出文件路径
	 * @prarm impOrClk 控制输出结果
	 * @function 媒体
	 */
	public  boolean SumMediaJob(String inputDir ,String outputDir,String impOrClk){
		boolean isSuccessful = false;
		conf.set("impOrClk", impOrClk);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("SumMediaJob");
			FileInputFormat.setInputPaths(job,CommonUtil.listInPutDir(inputDir));
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.SumMediaMap.class);
			job.setReducerClass(MapReduce.SumOrderAreaReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	public  boolean SumOrderCrowdJob(String inputDir ,String outputDir,String impOrClk){
		boolean isSuccessful = false;
		conf.set("impOrClk", impOrClk);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("SumOrderCrowdJob");
			FileInputFormat.setInputPaths(job,CommonUtil.listInPutDir(inputDir));
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.sumCrowdMap.class);
			job.setReducerClass(MapReduce.sumCrowdReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	/**
	 * 
	 * @param paths 输入文件路径
	 * @param queue 队列
	 * @param outputDir 输出文件路径
	 * @param impOrClk 控制输出结果
	 * @param metrics JOB输出字段
	 * @
	 * @function 媒体
	 */
	public  boolean CombineMediaJob(Path [] paths ,String outputDir,String metrics){
		boolean isSuccessful = false;
		conf.set("metrics", metrics);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("CombineMediaJob");
			FileInputFormat.setInputPaths(job,paths);
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.CombineMediaMap.class);
			job.setReducerClass(MapReduce.CombineOrderAreaReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	/**
	 * @param paths 输入文件路径
	 * @param queue 队列
	 * @param outputDir 输出路径
	 * @  
	 * @function 活动
	 */
	public  boolean AnalyOrderJob(Path [] paths  ,String outputDir, String orderID){
		boolean isSuccessful = false;
		conf.set("orderID", orderID);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("AnalyOrderJob");
			FileInputFormat.setInputPaths(job, paths );
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.AnalyOrderMap.class);
			job.setReducerClass(MapReduce.AnalyOrderReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	
	/**
	 * 
	 * @param inputDir 输出文件路径
	 * @param queue 队列
	 * @param outputDir 输出文件路径
	 * @prarm impOrClk 控制输出结果
	 * @function 活动
	 */
	public  boolean SumOrderJob(String inputDir ,String outputDir,String impOrClk){
		boolean isSuccessful = false;
		conf.set("impOrClk", impOrClk);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("SumOrderJob");
			FileInputFormat.setInputPaths(job,CommonUtil.listInPutDir(inputDir));
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.SumOrderMap.class);
			job.setReducerClass(MapReduce.SumOrderAreaReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	
	/**
	 * 
	 * @param paths 输入文件路径
	 * @param queue 队列
	 * @param outputDir 输出文件路径
	 * @param impOrClk 控制输出结果
	 * @param metrics 输出字段类型
	 * @
	 * @function 活动
	 */
	public  boolean CombineOrderJob(Path [] paths ,String outputDir ,String metrics){
		boolean isSuccessful = false;
		conf.set("metrics", metrics);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("CombineOrderJob");
			FileInputFormat.setInputPaths(job,paths);
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.CombineOrderMap.class);
			job.setReducerClass(MapReduce.CombineOrderAreaReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	
	/**
	 * 
	 * @param paths JOB文件输入路径
	 * @param queue JOB队列
	 * @param outputDir JOB输出路径
	 * @param orderID 活动ID
	 * @
	 * @function 分析广告报表
	 */
	public  boolean AnalyOrderitemJob(Path [] paths  ,String outputDir , String orderID){
		boolean isSuccessful = false;
		conf.set("orderID", orderID);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("AnalyOrderitemJob");
			FileInputFormat.setInputPaths(job, paths );
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.AnalyOrderItemMap.class);
			job.setReducerClass(MapReduce.AnalyInventoryReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	
	/**
	 * 
	 * @param paths JOB输入文件路径
	 * @param queue JOB队列
	 * @param outputDir JOB输出文件路径
	 * @param orderID 活动ID
	 * @ 
	 * @function 媒体地域报表
	 */
	public  boolean AnalyMediaAreaJob(Path [] paths  ,String outputDir, String orderID){
		boolean isSuccessful = false;
		conf.set("orderID", orderID);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("AnalyMediaAreaJob");
			FileInputFormat.setInputPaths(job, paths );
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.AnalyMediaAreaMap.class);
			job.setReducerClass(MapReduce.AnalyMediaAreaReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	/**
	 * 
	 * @param inputDir 输出文件路径
	 * @param queue 队列
	 * @param outputDir 输出文件路径
	 * @prarm impOrClk 控制输出结果
	 * @function 媒体地域分析
	 */
	public  boolean SumMediaAreaJob(String inputDir ,String outputDir,String impOrClk){
		boolean isSuccessful = false;
		conf.set("impOrClk", impOrClk);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("SumMediaAreaJob");
			FileInputFormat.setInputPaths(job,CommonUtil.listInPutDir(inputDir));
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.SumMediaAreaMap.class);
			job.setReducerClass(MapReduce.SumOrderAreaReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	/**
	 * 
	 * @param paths 输入文件路径
	 * @param queue 队列
	 * @param outputDir 输出文件路径
	 * @param impOrClk 控制输出结果
	 * @param metrics JOB输出队列
	 * @
	 * @function 媒体地域分析
	 */
	public  boolean CombineMediaAreaJob(Path [] paths ,String outputDir , String metrics){
		boolean isSuccessful = false;
		conf.set("metrics", metrics);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("CombineMediaAreaJob");
			FileInputFormat.setInputPaths(job,paths);
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.CombineMediaAreaMap.class);
			job.setReducerClass(MapReduce.CombineOrderAreaReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	
	/**
	 * 
	 * @param paths 输入文件路径
	 * @param queue 队列
	 * @param outputDir 输出文件路径
	 * @prarm impOrClk 控制输出结果
	 * @
	 * @function 分析指定页面数据
	 */
	public  boolean AnalyActJob(Path [] paths ,String outputDir , String actID){
		boolean isSuccessful = false;
		conf.set("actID", actID);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("AnalyActJob");
			FileInputFormat.setInputPaths(job,paths);
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.AnalyActMap.class);
			job.setReducerClass(MapReduce.AnalyActReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	
	/**
	 * 
	 * @param paths 输入文件路径
	 * @param queue 队列
	 * @param outputDir 输出文件路径
	 * @param metrics 控制输出结果
	 * @
	 * @function 计算指定页面数据
	 */
	public  boolean SumActJob(String  inputDir ,String outputDir , String metrics){
		boolean isSuccessful = false;
		conf.set("metrics", metrics);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("SumActJob");
			FileInputFormat.setInputPaths(job,CommonUtil.listInPutDir(inputDir));
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.SumActMap.class);
			job.setReducerClass(MapReduce.SumActReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	
	/**
	 * 
	 * @param paths 输入文件路径
	 * @param queue 队列
	 * @param outputDir 输出文件路径
	 * @prarm impOrClk 控制输出结果
	 * @
	 * @function 分析指定页面地域数据
	 */
	public  boolean AnalyActRegionJob(Path [] paths ,String outputDir , String actID){
		boolean isSuccessful = false;
		conf.set("actID", actID);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("AnalyActRegionJob");
			FileInputFormat.setInputPaths(job,paths);
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.AnalyActRegionMap.class);
			job.setReducerClass(MapReduce.AnalyActRegionReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	
	/**
	 *  
	 * @param inputDir Job输入路径
	 * @param queue Job 所在队列
	 * @param outputDir Job 输出路径
	 * @param metrics 输出字段
	 * @
	 * @function 计算指定页面地域数据
	 */
	public  boolean SumActRegionJob(String  inputDir ,String outputDir,String metrics){
		boolean isSuccessful = false;
		conf.set("metrics", metrics);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("SumActRegionJob");
			FileInputFormat.setInputPaths(job,CommonUtil.listInPutDir(inputDir));
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.SumActRegionMap.class);
			job.setReducerClass(MapReduce.SumActReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	
	/**
	 * 
	 * @param paths 输入文件路径
	 * @param queue 队列
	 * @param outputDir 输出文件路径
	 * @prarm impOrClk 控制输出结果
	 * @
	 * @function 分析指定厂商分小时浏览量
	 */
	public  boolean AnalyComapnyHourJob(Path [] paths ,String outputDir , String companyID){
		boolean isSuccessful = false;
		conf.set("companyID", companyID);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("AnalyComapnyHourJob");
			FileInputFormat.setInputPaths(job,paths);
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.AnalyCompanyHourMap.class);
			job.setReducerClass(MapReduce.AnalyCompanyHourReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	
	/**
	 * 
	 * @param paths 输入文件路径
	 * @param queue 队列
	 * @param outputDir 输出文件路径
	 * @prarm impOrClk 控制输出结果
	 * @
	 * @function 分析指定活动分小时浏览量
	 */
	public  boolean AnalyOrderHourJob(Path [] paths ,String outputDir , String companyID ,String orderID){
		boolean isSuccessful = false;
		conf.set("orderID", orderID);
		conf.set("companyID", companyID);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("AnalyOrderHourJob");
			FileInputFormat.setInputPaths(job,paths);
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.AnalyOrderHourMap.class);
			job.setReducerClass(MapReduce.AnalyCompanyHourReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	
	/**
	 * 
	 * @param paths 输入文件路径
	 * @param queue 队列
	 * @param outputDir 输出文件路径
	 * @param impOrClk 控制输出结果
	 * @
	 * @function 分析指定媒体分小时浏览量
	 */
	public  boolean AnalyMediaHourJob(Path [] paths ,String outputDir,String orderID){
		boolean isSuccessful = false;
		conf.set("orderID", orderID);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("AnalyMediaHourJob");
			FileInputFormat.setInputPaths(job,paths);
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.AnalyMediaHourMap.class);
			job.setReducerClass(MapReduce.AnalyCompanyHourReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	
	/**
	 * 
	 * @param paths 输入文件路径
	 * @param queue 队列
	 * @param outputDir 输出文件路径
	 * @prarm impOrClk 控制输出结果
	 * @
	 * @function 分析指定页面分小时浏览量
	 */
	public  boolean AnalyActHourJob(Path [] paths ,String outputDir , String actID){
		boolean isSuccessful = false;
		conf.set("actID", actID);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("AnalyActHourJob");
			FileInputFormat.setInputPaths(job,paths);
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.AnalyActHourMap.class);
			job.setReducerClass(MapReduce.AnalyCompanyHourReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	/**
	 * @param paths 输入文件路径
	 * @param queue 队列
	 * @param outputDir 输出路径
	 * @  
	 * @function 分析展现活动人群数据
	 */
	public  boolean AnalyOrderCrowdJob(Path [] paths  ,String outputDir , String orderID){
		boolean isSuccessful = false;
		conf.set("orderID", orderID);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("AnalyOrderCrowdJob");
			FileInputFormat.setInputPaths(job, paths );
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.AnalyOrderImpCrowdMap.class);
			job.setReducerClass(MapReduce.AnalyOrderImpCrowdReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	
	/**
	 * @param paths 输入文件路径
	 * @param queue 队列
	 * @param outputDir 输出路径
	 * @  
	 * @function 分析展现活动人群数据
	 */
	public  boolean AnalyOrderCrowdClkJob(Path [] paths  ,String outputDir  , String orderID){
		boolean isSuccessful = false;
		conf.set("orderID", orderID);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("AnalyOrderCrowdClkJob");
			FileInputFormat.setInputPaths(job, paths );
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.AnalyOrderClkCrowdMap.class);
			job.setReducerClass(MapReduce.AnalyMediaReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	
	/**
	 * 
	 * @param outputDir  任务输出路径
	 * @param queue 任务所属队列
	 * @param paths 任务输入文件路径
	 * @param orderId 活动ID
	 * @ 
	 */
	public  boolean AnalyOrderSiteCoincide(String outputDir ,Path [] paths ,String orderId ,String coincide ){
		boolean isSuccessful = false;
		conf.set("coincide", coincide);
		conf.set("orderID", orderId);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("AnalyOrderSiteCoincide");
			FileInputFormat.setInputPaths(job, paths);
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.AnalyOrderSiteCoincideMap.class);
			job.setReducerClass(MapReduce.AnalyOrderSiteCoincideReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}

	
	/**
	 * 
	 * @param inputDir 任务输入路径
	 * @param queue 队列
	 * @param outputDir 任务输出路径
	 * @ 
	 */
	public  boolean SumOrderSiteCoincide(String inputDir ,String outputDir){
		boolean isSuccessful = false;
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("SumOrderSiteCoincide");
			FileInputFormat.setInputPaths(job, CommonUtil.listInPutDir(inputDir));
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.SumOrderSiteCoincideMap.class);
			job.setReducerClass(MapReduce.SumOrderSiteCoincideReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	
	/**
	 * @paths 任务输入文件路径
	 * @param queue 指定队列
	 * @ 
	 * @fucntion 页面人群定制报表
	 */
	public  boolean AnalyActCrowdJob(Path [] paths ,String outputDir ,String actId ,String metrics){		
		boolean isSuccessful = false;
		conf.set("actId",actId);
		conf.set("metrics", metrics);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("AnalyActCrowdJob");
			FileInputFormat.setInputPaths(job, paths);
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.AnalyActCrowdMap.class);
			job.setReducerClass(MapReduce.AnalyActCrowdReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	/**
	 * @paths 任务输入文件路径
	 * @param queue 指定队列
	 * @ 
	 * @fucntion 页面人群定制报表
	 */
	public  boolean AnalySiteCrowdJob(Path [] paths ,String outputDir ,String siteId){		
		boolean isSuccessful = false;
		conf.set("siteId",siteId);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("AnalySiteCrowdJob");
			FileInputFormat.setInputPaths(job, paths);
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.AnalySiteCrowdMap.class);
			job.setReducerClass(MapReduce.AnalyseSiteCrowdReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	/**
	 * @paths 任务输入文件路径
	 * @param queue 指定队列
	 * @ 
	 * @fucntion 页面人群定制报表
	 */
	public  boolean sumSiteCrowdJob(String inputDir ,String outputDir,String metrics){		
		boolean isSuccessful = false;
		conf.set("metrics", metrics);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("sumSiteCrowdJob");
			FileInputFormat.setInputPaths(job, CommonUtil.listInPutDir(inputDir));
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.sumSiteCrowdMap.class);
			job.setReducerClass(MapReduce.sumSiteCrowdReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	/***
	 * 
	 * @param outputDir  任务输出路径
	 * @param queue 任务所属队列
	 * @param paths 任务输入文件路径
	 * @prama orderId 活动ID
	 * @ 
	 * @function 分析指定活动频次的PVUV数据
	 */
	public  boolean AnalyOrderFrequencyPVUV (String outputDir ,Path [] paths ,String orderId){
		boolean isSuccessful = false;
		conf.set("orderId", orderId);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("AnalyOrderFrequencyPVUV");
			FileInputFormat.setInputPaths(job, paths);
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.AnalyOrderFrequencyMap.class);
			job.setReducerClass(MapReduce.AnalyOrderFrequencyReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	
	
	/**
	 * 
	 * @param inputDir 任务输入路径
	 * @param queue 队列
	 * @param outputDir 任务输出路径
	 * @param frequency 频次
	 * @ 
	 * @function 计算活动频次的PV UV数据
	 */
	public  boolean SumOrderFrequencyPVUV(String inputDir ,String outputDir ,String frequency  ,String impOrClk){
		boolean isSuccessful = false;
		conf.set("frequency", frequency);
		conf.set("impOrClk", impOrClk);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("SumOrderFrequencyPVUV");
			FileInputFormat.setInputPaths(job, CommonUtil.listInPutDir(inputDir));
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.SumOrderFrequencyMap.class);
			job.setReducerClass(MapReduce.SumOrderFrequencyReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	
	/***
	 * 
	 * @param outputDir  任务输出路径
	 * @param queue 任务所属队列
	 * @param paths 任务输入文件路径
	 * @prama orderId 活动ID
	 * @ 
	 * @function 分析指定活动下媒体频次的PVUV数据
	 */
	public  boolean AnalyOrderSiteFrequency (String outputDir ,Path [] paths ,String orderId){
		boolean isSuccessful = false;
		conf.set("orderId", orderId);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("AnalyOrderSiteFrequency");
			FileInputFormat.setInputPaths(job, paths);
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.AnalyOrderSiteFrequencyMap.class);
			job.setReducerClass(MapReduce.AnalyOrderSiteFrequencyReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	
	/**
	 * 
	 * @param inputDir 任务输入路径
	 * @param queue 队列
	 * @param outputDir 任务输出路径
	 * @param frequency 频次
	 * @ 
	 * @function 计算活动频次的PV UV数据
	 */
	public  boolean SumOrderSiteFrequencyPVUV(String inputDir ,String outputDir ,String frequency  ,String impOrClk){
		boolean isSuccessful = false;
		conf.set("frequency", frequency);
		conf.set("impOrClk", impOrClk);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("SumOrderSiteFrequencyPVUV");
			FileInputFormat.setInputPaths(job, CommonUtil.listInPutDir(inputDir));
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.SumOrderSiteFrequencyMap.class);
			job.setReducerClass(MapReduce.SumOrderFrequencyReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	/**
	 * 
	 * @param outputDir Job 输出路径
	 * @param queue Job所在队列
	 * @param paths Job 输入路径
	 * @param orderId 活动ID
	 * @param urlType 区别是域名形式（1）还是包含网页完整地址（2） 
	 * @ 
	 * @function 指定活动下的广告来源
	 * @修改author zhou.yuanlong
	 * @修改date 2013-07-05
	 */
	public  boolean AnalyOrderItemReferralPv (String outputDir ,Path [] paths  ,String orderId, String urlType){
		boolean isSuccessful = false;
		conf.set("orderId", orderId);
		conf.set("urlType", urlType);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("AnalyOrderItemReferralPv");
			FileInputFormat.setInputPaths(job, paths);
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.AnalyOrderItemReferralMap.class);
			job.setReducerClass(MapReduce.AnalyOrderItemReferralReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	/**
	 * 
	 * @param inputDir 任务输入路径
	 * @param queue 队列
	 * @param outputDir 任务输出路径
	 * @ 
	 * @function 计算活动下地域数据
	 */
	public  boolean SumOrderItemReferralPv(String inputDir ,String outputDir){
		boolean isSuccessful = false;
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("SumOrderRegiontmp");
			FileInputFormat.setInputPaths(job, CommonUtil.listInPutDir(inputDir));
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.SumOrderRegionPvAndUvMap.class);
			job.setReducerClass(MapReduce.SumOrderRegionPvAndUvReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	/**
	 * 
	 * @param InputFileDate 任务输入日志日期
	 * @param orderId 广告活动
	 * @param date 计算饱和度日期
	 * @param queue 队列
	 * @param outputDir 任务输出路径
	 * @ 
	 */
	public  boolean AnalyseSaturationJob(Path [] path ,String orderId, String outputDir,String endDate)  {
		boolean isSuccessful = false;
		conf.set("endDate", endDate);
		conf.set("orderId", orderId);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("AnalyseSaturation");
			FileInputFormat.setInputPaths(job, path);
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.AnalyseSaturationMap.class);
			job.setReducerClass(MapReduce.AnalyseSaturationReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	
	/**
	 * 
	 * @param inputDir 任务输入路径
	 * @param queue 队列
	 * @param outputDir 任务输出路径
	 * @ 
	 */
	public  boolean SumSaturationJob(String inputDir ,String outputDir,String endDate){
		boolean isSuccessful = false;
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("SumSaturation");
			FileInputFormat.setInputPaths(job, CommonUtil.listInPutDir(inputDir));
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.SumSaturationMap.class);
			job.setReducerClass(MapReduce.SumSaturationReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	/**
	 * 
	 * @param outputDir 任务输出位置
	 * @param queue 队列
	 * @param paths 任务输入路径
	 * @param orderId 活动ID
	 * @
	 * @function 输出指定活动下的cookie
	 */
	public  boolean AnalyOrderCookie (String outputDir ,Path[] paths , String orderId ){
		boolean isSuccessful = false;
		conf.set("orderId", orderId);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("AnalyOrderCookie");
			FileInputFormat.setInputPaths(job, paths );
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.AnalyOrderCookieMap.class);
			job.setReducerClass(MapReduce.AnalyOrderCookieReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(NullWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	/**
	 * 
	 * @param outputDir 任务输出位置
	 * @param queue 队列
	 * @param paths 任务输入路径
	 * @param siteids 媒体ID
	 * @
	 * @function 分析全网浏览习惯
	 */
	public  boolean AnalyBrowsingHabits (String outputDir ,Path[] paths , String siteids ){
		boolean isSuccessful = false;
		conf.set("siteids", siteids);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("AnalyBrowsingHabits");
			FileInputFormat.setInputPaths(job, paths );
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.AnalyBrowsingHabitsMap.class);
			job.setReducerClass(MapReduce.AnalyBrowsingHabitsReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	/***
	 * 
	 * @param outputDir  任务输出路径
	 * @param queue 任务所属队列
	 * @param paths 任务输入文件路径
	 * @prama orderId 活动ID
	 * @ 
	 * @function 分析指定活动媒体广告位频次的PVUV数据
	 * @author zhou yuanlong
	 */
	public  boolean AnalyInventoryFrequencyPVUV (String outputDir ,Path [] paths ,String orderId, String mediaPar, String inventoryidPar, String collectPar){
		boolean isSuccessful = false;
		conf.set("orderId", orderId);
		conf.set("mediaPar", mediaPar);
		conf.set("inventoryidPar", inventoryidPar);
		conf.set("collectPar", collectPar);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("AnalyInventoryFrequencyPVUV");
			FileInputFormat.setInputPaths(job, paths);
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.AnalyInventoryFrequencyMap.class);
			job.setReducerClass(MapReduce.AnalyInventoryFrequencyReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	/**
	 * 
	 * @param inputDir 任务输入路径
	 * @param queue 队列
	 * @param outputDir 任务输出路径
	 * @param frequency 频次
	 * @ 
	 * @function 计算活动媒体广告位频次的PV UV数据
	 * @author zhou yuanlong
	 */
	public  boolean SumInventoryFrequencyPVUV(String inputDir ,String outputDir ,String frequency ,String impOrClk){
		boolean isSuccessful = false;
		conf.set("frequency", frequency);
		conf.set("impOrClk", impOrClk);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("SumInventoryFrequencyPVUV");
			FileInputFormat.setInputPaths(job, CommonUtil.listInPutDir(inputDir));
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.SumInventoryFrequencyMap.class);
			job.setReducerClass(MapReduce.SumInventoryFrequencyReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	/**
	 * 
	 * @param paths 输入文件路径
	 * @param queue 队列
	 * @param outputDir 输出文件路径
	 * @param impOrClk 控制输出结果
	 * @param metrics JOB输出字段
	 * @
	 * @function 合并广告位频次曝光点击数据
	 * @author zhou.yuanlong
	 * @date 2013-08-16
	 */
	public  boolean CombineInventoryFrequencyPvUvClkUcJob(Path [] paths ,String outputDir ,String metrics){
		boolean isSuccessful = false;
		conf.set("metrics", metrics);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("CombineInventoryFrequencyPvUvClkUc");
			FileInputFormat.setInputPaths(job,paths);
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.CombineInventoryFrequencyPvUvClkUcMap.class);
			job.setReducerClass(MapReduce.CombineInventoryFrequencyPvUvClkUcReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	/**
	 * 
	 * @param paths 输入文件路径
	 * @param queue 队列
	 * @param outputDir 输出文件路径
	 * @param impOrClk 控制输出结果
	 * @param metrics JOB输出字段
	 * @
	 * @function 合并活动频次曝光点击数据
	 * @author zhou.yuanlong
	 * @date 2013-08-16
	 */
	public  boolean CombineOrderFrequencyPvUvClkUcJob(Path [] paths ,String outputDir ,String metrics){
		boolean isSuccessful = false;
		conf.set("metrics", metrics);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("CombineOrderFrequencyPvUvClkUc");
			FileInputFormat.setInputPaths(job,paths);
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.CombineOrderFrequencyPvUvClkUcMap.class);
			job.setReducerClass(MapReduce.CombineOrderFrequencyPvUvClkUcReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	/**
	 * 
	 * @param paths 输入文件路径
	 * @param queue 队列
	 * @param outputDir 输出文件路径
	 * @param impOrClk 控制输出结果
	 * @param metrics JOB输出字段
	 * @
	 * @function 合并活动媒体频次曝光点击数据
	 * @author zhou.yuanlong
	 * @date 2013-08-16
	 */
	public  boolean CombineOrderSiteFrequencyPvUvClkUcJob(Path [] paths ,String outputDir ,String metrics){
		boolean isSuccessful = false;
		conf.set("metrics", metrics);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("CombineOrderSiteFrequencyPvUvClkUc");
			FileInputFormat.setInputPaths(job,paths);
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.CombineInventoryFrequencyPvUvClkUcMap.class);
			job.setReducerClass(MapReduce.CombineInventoryFrequencyPvUvClkUcReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	/**
	 * 
	 * @param paths 输入文件路径
	 * @param queue 队列
	 * @param outputDir 输出文件路径
	 * @param impOrClk 控制输出结果
	 * @param frequencyIndex 频次字段所在列的位置 从0开始计数
	 * @param inputLineLength 每列字段的个数从1开始计数
	 * @
	 * @function 根据频次前面的数据分组数据，且根据频次排序数据  及实现group by 和 order by asc
	 * @author zhou.yuanlong
	 * @date 2013-08-21
	 */
	public  boolean SortFrequencyPvUvClkUcJob(Path [] paths ,String outputDir ,int frequencyIndex ,int inputLineLength){
		boolean isSuccessful = false;
		conf.setInt("frequencyIndex", frequencyIndex);//注意：是setInt不是set
		conf.setInt("inputLineLength", inputLineLength);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("secondarySort");
			FileInputFormat.setInputPaths(job,paths);
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.SecondarySortMap.class);
			job.setReducerClass(MapReduce.SecondarySortReduce.class);
			job.setMapOutputKeyClass(MapReduce.TextIntPair.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setPartitionerClass(MapReduce.FirstPartitioner.class);// 分区函数
			job.setGroupingComparatorClass(MapReduce.GroupingComparator.class);// 分组函数
			job.setInputFormatClass(TextInputFormat.class); // 将输入的数据集分割成小数据块splites，同时提供一个RecordReder的实现
			job.setOutputFormatClass(TextOutputFormat.class);// 提供一个RecordWriter的实现，负责数据输出
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	
	
	/******************************************************************************************************************
	 * @TODO 去重的曝光频次统计
	 * @author wang meng(uni)
	 * @date 2013-10-25 16:16
	 * 
	 */
	public  boolean AnalyseUniFrequencyJob(String beginDate , String endDate ,Path [] paths  ,String outputDir,String orderid)  {
		boolean isSuccessful = false;
		conf.set("beginDate", beginDate);
		conf.set( "endDate",endDate);
		conf.set("orderid", orderid);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("AnalyseUniFrequencyJob");
			FileInputFormat.setInputPaths(job, paths );
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.AnalyseUniFrequencyMap.class);
			job.setReducerClass(MapReduce.AnalyseUniFrequencyReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	public  boolean SumUniFrequencyJob(String inputDir,String outputDir) {
		boolean isSuccessful = false;
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("SumUniFrequencyJob");
			FileInputFormat.setInputPaths(job,CommonUtil.listInPutDir(inputDir));
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.SumUniFrequencyMap.class);
			job.setReducerClass(MapReduce.SumUniFrequencyReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
				
	}
	
	public  boolean SortFrequencyJob(String inputDir ,String outputDir ,int frequencyIndex ,int inputLineLength){
		boolean isSuccessful = false;
		conf.setInt("frequencyIndex", frequencyIndex);//注意：是setInt不是set
		conf.setInt("inputLineLength", inputLineLength);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("secondarySort");
			FileInputFormat.setInputPaths(job,CommonUtil.listInPutDir(inputDir));
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.SecondarySortMap.class);
			job.setReducerClass(MapReduce.SecondarySortReduce.class);
			job.setMapOutputKeyClass(MapReduce.TextIntPair.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setPartitionerClass(MapReduce.FirstPartitioner.class);// 分区函数
			job.setGroupingComparatorClass(MapReduce.GroupingComparator.class);// 分组函数
			job.setInputFormatClass(TextInputFormat.class); // 将输入的数据集分割成小数据块splites，同时提供一个RecordReder的实现
			job.setOutputFormatClass(TextOutputFormat.class);// 提供一个RecordWriter的实现，负责数据输出
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	/**
	 * 
	 * @param paths 任务输入文件路径 
	 * @param queue   任务所在队列
	 * @param outputDir 任务输出路径
	 * @
	 * @function   分析网站移动设备报告任务配置  分析UA的
	 * @date 2013-12-17
	 * @zhou.yuanlong
	 */
	public  boolean AnalySiteMobileDevicesUAJob(Path [] paths  ,String outputDir, String site, String flag){
		boolean isSuccessful = false;
		conf.set("site",site);
		conf.set("flag",flag);
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("AnalySiteMobileDevicesUAJob");
			FileInputFormat.setInputPaths(job, paths );
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.AnalySiteMobileDevicesUAMap.class);
			job.setReducerClass(MapReduce.AnalySiteMobileDevicesUAReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	/**
	 * 
	 * @param paths 任务输入文件路径 
	 * @param queue   任务所在队列
	 * @param outputDir 任务输出路径
	 * @
	 * @function   分析网站移动设备报告任务配置
	 * @date 2013-12-17
	 * @zhou.yuanlong
	 */
	public  boolean AnalySiteMobileDevicesJob(String inputDir  ,String outputDir){
		boolean isSuccessful = false;
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("AnalySiteMobileDevicesJob");
			FileInputFormat.setInputPaths(job, CommonUtil.listInPutDir(inputDir) );
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.AnalySiteMobileDevicesMap.class);
			job.setReducerClass(MapReduce.AnalySiteMobileDevicesReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
	
	
	/**
	 * 
	 * @param paths 任务输入文件路径 
	 * @param queue   任务所在队列
	 * @param outputDir 任务输出路径
	 * @
	 * @function  分析网站移动设备报告任务配置
	 * @date 2013-12-17
	 * @zhou.yuanlong
	 */
	public  boolean SumSiteMobileDevicesJob(String inputDir  ,String outputDir){
		boolean isSuccessful = false;
		try {
			Job job = new Job(conf);
			job.setJarByClass(JobControl.class);
			job.setJobName("SumSiteMobileDevicesJob");
			FileInputFormat.setInputPaths(job, CommonUtil.listInPutDir(inputDir) );
			FileOutputFormat.setOutputPath(job, new Path(outputDir));
			job.setMapperClass(MapReduce.SumSiteMobileDevicesMap.class);
			job.setReducerClass(MapReduce.SumSiteMobileDevicesReduce.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			isSuccessful = job.waitForCompletion(true);
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return isSuccessful;
	}
}
