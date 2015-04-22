package com.uniclick.databank.mapreduce.job;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uniclick.databank.mapreduce.common.CommonUtil;
import com.uniclick.databank.mapreduce.job.iface.RunCustomJob;
import com.uniclick.databank.mapreduce.pojo.MapReduceJobOptionModel;
import com.uniclick.databank.mapreduce.util.JobOptionsParse;


/**
 * 
 * @author yang qi
 * @date 2012-06-19
 * @version 1.0
 * @function 分析指定活动下的人群信息
 */
public class RunOrderCrowdJob implements RunCustomJob{
	private static final Logger logger = LoggerFactory.getLogger(RunOrderCrowdJob.class);
	private JobControl jobControl;
	
	public RunOrderCrowdJob(String queue) {
		jobControl = new JobControl(queue);
	}
	
	public boolean run(String beginDate,String endDate,String jobOptins,String resultnorm) {
		boolean isSuccessful = true;
		MapReduceJobOptionModel mrOptionModel = JobOptionsParse.parse(jobOptins);
		boolean impStatus = CommonUtil.beSureAnalyLogType(resultnorm, "imp");
		boolean clkStatus = CommonUtil.beSureAnalyLogType(resultnorm, "clk");
		String strDate = CommonUtil.strStartAndEndTime(beginDate, endDate);//返回日志时间
		String analyOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/AnalyCustomizeOrderCrowd/output";
		String sumImpOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/SumCustomizeOrderCrowdImp/output";
		String sumClkOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/SumCustomizeOrderCrowdClk/output";
		String combineOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/CombineCustomizeOrderCrowd/output";
		Path [] paths = null ;
		if(impStatus&&clkStatus){//分析展现点击日志
			/**
			 * 分析展现日志
			 */
			paths = CommonUtil.impLog(strDate);
			if(paths==null||paths.length==0){
				logger.warn("have no log for imp in " + strDate);
				return false;
			}else{
				if(CommonUtil.judgeDir(analyOutputDir)){
					CommonUtil.delete(analyOutputDir);
				}
				if(!jobControl.AnalyOrderCrowdJob(paths, analyOutputDir, mrOptionModel.getOrder())){
					logger.error("RunOrderCrowdJob AnalyOrderCrowdJob imp proccess is faild");
					return false;
				}
				if(CommonUtil.judgeDir(sumImpOutputDir)){
					CommonUtil.delete(sumImpOutputDir);
				}
				if(!jobControl.SumOrderCrowdJob(analyOutputDir, sumImpOutputDir, "imp")){
					logger.error("RunOrderCrowdJob SumOrderCrowdJob imp proccess is faild");
					return false;
				}
			}
			/**
			 * 分析点击日志
			 */
			paths = CommonUtil.clkLog(strDate);
			if(paths==null||paths.length==0){
				logger.warn("have no log for clk in " + strDate);
				return false;
			}else{
				if(CommonUtil.judgeDir(analyOutputDir)){
					CommonUtil.delete(analyOutputDir);
				}
				if(!jobControl.AnalyOrderCrowdJob(paths, analyOutputDir, mrOptionModel.getOrder())){
					logger.error("RunOrderCrowdJob AnalyOrderCrowdJob clk proccess is faild");
					return false;
				}
				if(CommonUtil.judgeDir(sumClkOutputDir)){
					CommonUtil.delete(sumClkOutputDir);
				}
				if(!jobControl.SumOrderCrowdJob(analyOutputDir, sumClkOutputDir, "clk")){
					logger.error("RunOrderCrowdJob SumOrderCrowdJob clk proccess is faild");
					return false;
				}
			}
			/**
			 * 合并
			 */
			paths = CommonUtil.impAndClkLog(sumImpOutputDir, sumClkOutputDir);
			if(paths==null||paths.length==0){
				logger.warn("sumImpOutputDir : " + sumImpOutputDir + 
						"\n sumClkOutputDir : " + sumClkOutputDir + " is not exist");
				return false;
			}else{
				if(CommonUtil.judgeDir(combineOutputDir)){
					CommonUtil.delete(combineOutputDir);
				}
				if(!jobControl.CombineMediaJob(paths, combineOutputDir,resultnorm)){
					logger.error("RunOrderCrowdJob CombineMediaJob proccess is faild");
					return false;
				}
				//String fileDate = beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate;
				//CommonUtil.ftpToLocal(fileDate, combineOutputDir, "orderCrowd",type);
			}
		}else if(impStatus){//分析展现日志
			paths = CommonUtil.impLog(strDate);
			if(paths==null||paths.length==0){
				logger.warn("have no log for imp in " + strDate);
				return false;
			}else{
				if(CommonUtil.judgeDir(analyOutputDir)){
					CommonUtil.delete(analyOutputDir);
				}
				if(!jobControl.AnalyOrderCrowdJob(paths, analyOutputDir, mrOptionModel.getOrder())){
					logger.error("RunOrderCrowdJob AnalyOrderCrowdJob imp proccess is faild");
					return false;
				}
				if(CommonUtil.judgeDir(sumImpOutputDir)){
					CommonUtil.delete(sumImpOutputDir);
				}
				if(!jobControl.SumOrderCrowdJob(analyOutputDir, sumImpOutputDir, "imp")){
					logger.error("RunOrderCrowdJob SumOrderCrowdJob imp proccess is faild");
					return false;
				}
				//删除点击输出路径
				if(CommonUtil.judgeDir(sumClkOutputDir)){
					CommonUtil.delete(sumClkOutputDir);
				}
				//合并
				paths = CommonUtil.impAndClkLog(sumImpOutputDir, sumClkOutputDir);
				if(paths==null||paths.length==0){
					logger.warn("sumImpOutputDir : " + sumImpOutputDir + 
							"\n sumClkOutputDir : " + sumClkOutputDir + " is not exist");
					return false;
				}else{
					if(CommonUtil.judgeDir(combineOutputDir)){
						CommonUtil.delete(combineOutputDir);
					}
					if(!jobControl.CombineMediaJob(paths, combineOutputDir,resultnorm)){
						logger.error("RunOrderCrowdJob CombineMediaJob proccess is faild");
						return false;
					}
					//CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, combineOutputDir, "orderCrowd",type);
				}
			}
		}else if(clkStatus){//分析点击日志
			paths = CommonUtil.clkLog(strDate);
			if(paths==null||paths.length==0){
				logger.warn("have no log for clk in " + strDate);
				return false;
			}else{
				if(CommonUtil.judgeDir(analyOutputDir)){
					CommonUtil.delete(analyOutputDir);
				}
				if(!jobControl.AnalyOrderCrowdJob(paths, analyOutputDir, mrOptionModel.getOrder())){
					logger.error("RunOrderCrowdJob AnalyOrderCrowdJob clk proccess is faild");
					return false;
				}
				if(CommonUtil.judgeDir(sumClkOutputDir)){
					CommonUtil.delete(sumClkOutputDir);
				}
				if(!jobControl.SumOrderCrowdJob(analyOutputDir, sumClkOutputDir, "clk")){
					logger.error("RunOrderCrowdJob SumOrderCrowdJob clk proccess is faild");
					return false;
				}
				//删除展现输出路径
				if(CommonUtil.judgeDir(sumImpOutputDir)){
					CommonUtil.delete(sumImpOutputDir);
				}
				//合并
				paths = CommonUtil.impAndClkLog(sumImpOutputDir, sumClkOutputDir);
				if(paths==null||paths.length==0){
					logger.warn("sumImpOutputDir : " + sumImpOutputDir + 
							"\n sumClkOutputDir : " + sumClkOutputDir + " is not exist");
					return false;
				}else{
					if(CommonUtil.judgeDir(combineOutputDir)){
						CommonUtil.delete(combineOutputDir);
					}
					if(!jobControl.CombineMediaJob(paths, combineOutputDir,resultnorm)){
						logger.error("RunOrderCrowdJob CombineMediaJob proccess is faild");
						return false;
					}
					//CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, combineOutputDir, "orderCrowd",type);
				}
			}
		}
		return isSuccessful;
	}
/*	public static void main(String args[])throws Exception{
		String beginDate = null;
		String endDate  = null ; 
		String queue = "queue3"; 
		String orderID = null;
		String impOrClk  = null;//区别分析点击日志或是展现日志
		String type = "customize";
		if(args==null|| args.length==0){
		    System.out.println("[-B <beginTime>]" +
                    "[-E <endTime>] " +
                    "[-Q <queue>] "+
                    "[-IOC <imp or clk log>]"+
                    "[-OD <orderID>]");
		    System.exit(0);
		}else{
			for(int i=0 ;i<args.length ; i++){
				if("-B".equals(args[i])){
					beginDate = args[++i];
				}else if("-E".equals(args[i])){
					endDate = args[++i];
				}else if("-Q".equals(args[i])){
					queue = args[++i];
				}else if("-IOC".equals(args[i])){
					impOrClk = args[++i];
				}else if("-OD".equals(args[i])){
					orderID = args[++i];
				}
			}
		}
		boolean impStatus = CommonUtil.beSureAnalyLogType(impOrClk, "imp");
		boolean clkStatus = CommonUtil.beSureAnalyLogType(impOrClk, "clk");
		String strDate = CommonUtil.strStartAndEndTime(beginDate, endDate);//返回日志时间
		String analyOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/AnalyCustomizeOrderCrowd/output";
		String sumImpOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/SumCustomizeOrderCrowdImp/output";
		String sumClkOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/SumCustomizeOrderCrowdClk/output";
		String combineOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/CombineCustomizeOrderCrowd/output";
		Path [] paths = null ;
		if(impStatus&&clkStatus){//分析展现点击日志
			*//**
			 * 分析展现日志
			 *//*
			paths = CommonUtil.impLog(strDate);
			if(paths==null||paths.length==0){
				System.out.println("have no log");
				System.exit(0);
			}else{
				if(CommonUtil.judgeDir(analyOutputDir)){
					CommonUtil.delete(analyOutputDir);
				}
				JobControl.AnalyOrderCrowdJob(paths, analyOutputDir, orderID);
				if(CommonUtil.judgeDir(sumImpOutputDir)){
					CommonUtil.delete(sumImpOutputDir);
				}
				JobControl.SumOrderCrowdJob(analyOutputDir, sumImpOutputDir, "imp");
			}
			*//**
			 * 分析点击日志
			 *//*
			paths = CommonUtil.clkLog(strDate);
			if(paths==null||paths.length==0){
				System.out.println("have no log");
				System.exit(0);
			}else{
				if(CommonUtil.judgeDir(analyOutputDir)){
					CommonUtil.delete(analyOutputDir);
				}
				JobControl.AnalyOrderCrowdJob(paths, analyOutputDir, orderID);
				if(CommonUtil.judgeDir(sumClkOutputDir)){
					CommonUtil.delete(sumClkOutputDir);
				}
				JobControl.SumOrderCrowdJob(analyOutputDir, sumClkOutputDir, "clk");
			}
			*//**
			 * 合并
			 *//*
			paths = CommonUtil.impAndClkLog(sumImpOutputDir, sumClkOutputDir);
			if(paths==null||paths.length==0){
				System.out.println("have no log");
				System.exit(0);
			}else{
				if(CommonUtil.judgeDir(combineOutputDir)){
					CommonUtil.delete(combineOutputDir);
				}
				JobControl.CombineMediaJob(paths, combineOutputDir,impOrClk);
				String fileDate = beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate;
				CommonUtil.ftpToLocal(fileDate, combineOutputDir, "orderCrowd",type);
			}
		}else if(impStatus){//分析展现日志
			paths = CommonUtil.impLog(strDate);
			if(paths==null||paths.length==0){
				System.out.println("have no log");
				System.exit(0);
			}else{
				if(CommonUtil.judgeDir(analyOutputDir)){
					CommonUtil.delete(analyOutputDir);
				}
				JobControl.AnalyOrderCrowdJob(paths, analyOutputDir, orderID);
				if(CommonUtil.judgeDir(sumImpOutputDir)){
					CommonUtil.delete(sumImpOutputDir);
				}
				JobControl.SumOrderCrowdJob(analyOutputDir, sumImpOutputDir, "imp");
				//删除点击输出路径
				if(CommonUtil.judgeDir(sumClkOutputDir)){
					CommonUtil.delete(sumClkOutputDir);
				}
				//合并
				paths = CommonUtil.impAndClkLog(sumImpOutputDir, sumClkOutputDir);
				if(paths==null||paths.length==0){
					System.out.println("have no log");
					System.exit(0);
				}else{
					if(CommonUtil.judgeDir(combineOutputDir)){
						CommonUtil.delete(combineOutputDir);
					}
					JobControl.CombineMediaJob(paths, combineOutputDir,impOrClk);
					CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, combineOutputDir, "orderCrowd",type);
				}
				System.exit(0);
			}
		}else if(clkStatus){//分析点击日志
			paths = CommonUtil.clkLog(strDate);
			if(paths==null||paths.length==0){
				System.out.println("have no log");
				System.exit(0);
			}else{
				if(CommonUtil.judgeDir(analyOutputDir)){
					CommonUtil.delete(analyOutputDir);
				}
				JobControl.AnalyOrderCrowdClkJob(paths, analyOutputDir, orderID);
				if(CommonUtil.judgeDir(sumClkOutputDir)){
					CommonUtil.delete(sumClkOutputDir);
				}
				JobControl.SumOrderCrowdJob(analyOutputDir, sumClkOutputDir, "clk");
				//删除展现输出路径
				if(CommonUtil.judgeDir(sumImpOutputDir)){
					CommonUtil.delete(sumImpOutputDir);
				}
				//合并
				paths = CommonUtil.impAndClkLog(sumImpOutputDir, sumClkOutputDir);
				if(paths==null||paths.length==0){
					System.out.println("have no log");
					System.exit(0);
				}else{
					if(CommonUtil.judgeDir(combineOutputDir)){
						CommonUtil.delete(combineOutputDir);
					}
					JobControl.CombineMediaJob(paths, combineOutputDir,impOrClk);
					CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, combineOutputDir, "orderCrowd",type);
				}
				System.exit(0);
			}
		}
	}
*/
}
