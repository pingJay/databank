package com.uniclick.databank.mapreduce.job;


import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uniclick.databank.mapreduce.common.CommonUtil;
import com.uniclick.databank.mapreduce.job.iface.RunCustomJob;
import com.uniclick.databank.mapreduce.pojo.MapReduceJobOptionModel;
import com.uniclick.databank.mapreduce.util.JobOptionsParse;

/**
 * 类名称 ：RunUniFrequencyJob
 * 类描述：分析曝光频次覆盖
 * @author wang meng(uni)
 *
 */
public class RunUniFrequencyJob implements RunCustomJob{
	private static final Logger logger = LoggerFactory.getLogger(RunUniFrequencyJob.class);
	private JobControl jobControl;
	
	public RunUniFrequencyJob(String queue) {
		jobControl = new JobControl(queue);
	}
	
	public boolean run(String beginDate,String endDate,String jobOptins,String resultnorm) {
		boolean isSuccessful = true;
		MapReduceJobOptionModel mrOptionModel = JobOptionsParse.parse(jobOptins);
		String strDate = CommonUtil.strStartAndEndTime(beginDate, endDate);//返回日志时间
		String analyOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/AnalyseUniFrequency/output" ;
		String sumOutputDir ="hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/SumUniFrequency/output";
		String sortMergeOutPutDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/sortMergeUniFrequency/output";
		Path [] paths = CommonUtil.impLog(strDate) ;
		if(paths==null||paths.length==0){
			logger.warn("have no log for site in " + strDate);
			return false;
		}else{
			if(CommonUtil.judgeDir(analyOutputDir)){
				CommonUtil.delete(analyOutputDir);
			}
			if(!jobControl.AnalyseUniFrequencyJob(beginDate,endDate ,paths, analyOutputDir,mrOptionModel.getOrder())){
				logger.error("RunUniFrequencyJob AnalyseUniFrequencyJob proccess is faild");
				return false;
			}
			if(CommonUtil.judgeDir(sumOutputDir)){
				CommonUtil.delete(sumOutputDir);
			}
			if(!jobControl.SumUniFrequencyJob(analyOutputDir, sumOutputDir)){
				logger.error("RunUniFrequencyJob SumUniFrequencyJob proccess is faild");
				return false;
			}
			if(CommonUtil.judgeDir(sortMergeOutPutDir)){
				CommonUtil.delete(sortMergeOutPutDir);
			}
			int frequencyIndex = 1;
			int inputLineLength =4; 
			if(!jobControl.SortFrequencyJob(sumOutputDir, sortMergeOutPutDir , frequencyIndex, inputLineLength)){
				logger.error("RunUniFrequencyJob SortFrequencyJob proccess is faild");
				return false;
			}
			//CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, sortMergeOutPutDir,  "UniFreCustomize", type);
		}
		return isSuccessful;
	}
/*	public static void main(String[] args) throws Exception{
		String beginDate = null;
		String endDate  = null ; 
		String type = "customize";
		String queue = "queue3";
		String orderid = null;
		if(args==null|| args.length==0){
		    System.out.println("[-B <beginTime>]" +
	                "[-E <endTime>] " +
	                "[-Q <queue>] ");
		    System.exit(0);
		}else{
			for(int i=0 ;i<args.length ; i++){
				if("-B".equals(args[i])){
					beginDate = args[++i];
				}else if("-E".equals(args[i])){
					endDate = args[++i];
				}else if("-Q".equals(args[i])){
					queue = args[++i];
				}else if("-OD".equals(args[i])){
					orderid = args[++i];
				}
		     }
		}
		
		String strDate = CommonUtil.strStartAndEndTime(beginDate, endDate);//返回日志时间
		String analyOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/AnalyseUniFrequency/output" ;
		String sumOutputDir ="hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/SumUniFrequency/output";
		String sortMergeOutPutDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/sortMergeUniFrequency/output";
		Path [] paths = CommonUtil.impLog(strDate) ;
		if(paths==null||paths.length==0){
			System.out.println("have no log");
			System.exit(0);
		}else{
			if(CommonUtil.judgeDir(analyOutputDir)){
				CommonUtil.delete(analyOutputDir);
			}
			JobControl.AnalyseUniFrequencyJob(beginDate,endDate ,paths, analyOutputDir,orderid);
			if(CommonUtil.judgeDir(sumOutputDir)){
				CommonUtil.delete(sumOutputDir);
			}
			JobControl.SumUniFrequencyJob(analyOutputDir, sumOutputDir);
			//CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, sumOutputDir, "UniFreCustomize", type);
			//System.exit(0);
			if(CommonUtil.judgeDir(sortMergeOutPutDir)){
				CommonUtil.delete(sortMergeOutPutDir);
			}
			int frequencyIndex = 1;
			int inputLineLength =4; 
			JobControl.SortFrequencyJob(sumOutputDir, sortMergeOutPutDir , frequencyIndex, inputLineLength);
			CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, sortMergeOutPutDir,  "UniFreCustomize", type);
			System.exit(0);
		}
		
		*//**
		if(paths==null||paths.length==0){
			System.out.println("have no log");
			System.exit(0);
		}else{
			if(CommonUtil.judgeDir(sortMergeOutPutDir)){
				CommonUtil.delete(sortMergeOutPutDir);
			}
			int frequencyIndex = 2;
			int inputLineLength = 7; 
			JobControl.SortFrequencyPvUvClkUcJob(paths, sortMergeOutPutDir , frequencyIndex, inputLineLength);
			//分析结果本地存放位置
			String localDir = "OrderSiteFrequency";
			CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, sortMergeOutPutDir,  "UniFreCustomize", type);
		}
		
		**//*
	}
 */
}
