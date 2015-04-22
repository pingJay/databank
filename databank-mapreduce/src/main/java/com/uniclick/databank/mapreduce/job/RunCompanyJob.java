package com.uniclick.databank.mapreduce.job;

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
 * @date 2012-06-15
 * @version 1.0
 * @function 定制厂商报表
 */
public class RunCompanyJob implements RunCustomJob{
	private static final Logger logger = LoggerFactory.getLogger(RunCompanyJob.class);
	private JobControl jobControl;
	
	public RunCompanyJob(String queue) {
		jobControl = new JobControl(queue);
	}
	
	public boolean run(String beginDate,String endDate,String jobOptins,String resultnorm) {
		boolean isSuccessful = true;
		MapReduceJobOptionModel mrOptionModel = JobOptionsParse.parse(jobOptins);
		boolean impStatus = CommonUtil.beSureAnalyLogType(resultnorm, "imp");
		boolean clkStatus = CommonUtil.beSureAnalyLogType(resultnorm, "clk");
		String strDate = CommonUtil.strStartAndEndTime(beginDate, endDate);//返回日志时间
		String analyOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/AnalyCustomizeCompany/output";
		String sumImpOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/SumCustomizeCompanyImp/output";
		String sumClkOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/SumCustomizeCompanyClk/output";
		String combineOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/CombineCustomizeCompany/output";
		Path [] paths = null ;
		if(impStatus&&clkStatus){//分析展现和点击日志
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
				/*
				 * 分析第一步
				 */
				if(!jobControl.AnalyCompanyJob(paths, analyOutputDir,mrOptionModel.getCompany())) {
					logger.error("RunCompanyJob AnalyCompanyJob imp proccess is faild");
					return false;
				}
				
				if(CommonUtil.judgeDir(sumImpOutputDir)){
					CommonUtil.delete(sumImpOutputDir);
				}
				
				if(!jobControl.SumCompanyJob(analyOutputDir, sumImpOutputDir, "imp")) {
					logger.error("RunCompanyJob SumCompanyJob imp proccess is faild");
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
				
				if(!jobControl.AnalyCompanyJob(paths, analyOutputDir,mrOptionModel.getCompany())){
					logger.error("RunCompanyJob AnalyCompanyJob clk proccess is faild");
					return false;
				}
				if(CommonUtil.judgeDir(sumClkOutputDir)){
					CommonUtil.delete(sumClkOutputDir);
				}
				if(!jobControl.SumCompanyJob(analyOutputDir, sumClkOutputDir, "clk")) {
					logger.error("RunCompanyJob SumCompanyJob clk proccess is faild");
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
				
				if(!jobControl.CombineCompanyJob(paths, combineOutputDir,resultnorm)) {
					logger.error("RunCompanyJob CombineCompanyJob proccess is faild");
					return false;
				}
				//CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, combineOutputDir, "company",TYPE );
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
				if(!jobControl.AnalyCompanyJob(paths, analyOutputDir,mrOptionModel.getCompany())) {
					logger.error("RunCompanyJob AnalyCompanyJob imp proccess is faild");
					return false;
				}
				if(CommonUtil.judgeDir(sumImpOutputDir)){
					CommonUtil.delete(sumImpOutputDir);
				}
				if(!jobControl.SumCompanyJob(analyOutputDir, sumImpOutputDir, resultnorm)){
					logger.error("RunCompanyJob SumCompanyJob imp proccess is faild");
					return false;
				}
				//删除点击日志输出目录原有数据
				if(CommonUtil.judgeDir(sumClkOutputDir)){
					CommonUtil.delete(sumClkOutputDir);
				}
				//读取点击展现输出目录中的数据
				paths = CommonUtil.impAndClkLog(sumImpOutputDir, sumClkOutputDir);
				if(paths==null||paths.length==0){
					logger.warn("sumImpOutputDir : " + sumImpOutputDir + 
							"\n sumClkOutputDir : " + sumClkOutputDir + " is not exist");
					return false;
				}else{
					if(CommonUtil.judgeDir(combineOutputDir)){
						CommonUtil.delete(combineOutputDir);
					}
					
					if(!jobControl.CombineCompanyJob(paths, combineOutputDir,resultnorm)) {
						logger.error("RunCompanyJob CombineCompanyJob proccess is faild");
						return false;
					}
					//CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, combineOutputDir, "company",TYPE);
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
				if(!jobControl.AnalyCompanyJob(paths, analyOutputDir,mrOptionModel.getCompany())) {
					logger.error("RunCompanyJob AnalyCompanyJob clk proccess is faild");
					return false;
				}
				if(CommonUtil.judgeDir(sumClkOutputDir)){
					CommonUtil.delete(sumClkOutputDir);
				}
				if(!jobControl.SumCompanyJob(analyOutputDir, sumClkOutputDir, resultnorm)){
					logger.error("RunCompanyJob SumCompanyJob clk proccess is faild");
					return false;
				}
				//删除展现输出目录下的数据
				if(CommonUtil.judgeDir(sumImpOutputDir)){
					CommonUtil.delete(sumImpOutputDir);
				}
				//读取点击展现输出目录中的数据
				paths = CommonUtil.impAndClkLog(sumImpOutputDir, sumClkOutputDir);
				if(paths==null||paths.length==0){
					logger.warn("sumImpOutputDir : " + sumImpOutputDir + 
							"\n sumClkOutputDir : " + sumClkOutputDir + " is not exist");
					return false;
				}else{
					if(CommonUtil.judgeDir(combineOutputDir)){
						CommonUtil.delete(combineOutputDir);
					}
					if(!jobControl.CombineCompanyJob(paths, combineOutputDir,resultnorm)) {
						logger.error("RunCompanyJob CombineCompanyJob proccess is faild");
						return false;
					}
					//CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, combineOutputDir, "company",TYPE);
				}
			}
		}
		return isSuccessful;
	}
	
/*	public static void main(String args[])throws Exception{
		String beginDate = null;
		String endDate  = null ; 
		String queue = "queue3"; 
		String companyID = "0" ;
		String impOrClk  = null;//区别分析点击日志或是展现日志
		String type = "customize";
		if(args==null|| args.length==0){
		    System.out.println("[-B <beginTime>]" +
                    "[-E <endTime>] " +
                    "[-Q <queue>] "+
                    "[-IOC<imp or clk log>]"+
                    "[-CompanyID <compantId> ]");
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
				}else if("-CompanyID".equals(args[i])){
					companyID = args[++i];
				}
			}
		}
		boolean impStatus = false;
		boolean clkStatus = true;
		impStatus = CommonUtil.beSureAnalyLogType(impOrClk, "imp");
		clkStatus = CommonUtil.beSureAnalyLogType(impOrClk, "clk");
		String strDate = CommonUtil.strStartAndEndTime(beginDate, endDate);//返回日志时间
		String analyOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/AnalyCustomizeCompany/output";
		String sumImpOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/SumCustomizeCompanyImp/output";
		String sumClkOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/SumCustomizeCompanyClk/output";
		String combineOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/CombineCustomizeCompany/output";
		Path [] paths = null ;
		if(impStatus&&clkStatus){//分析展现和点击日志
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
				JobControl.AnalyCompanyJob(paths, analyOutputDir,companyID);
				if(CommonUtil.judgeDir(sumImpOutputDir)){
					CommonUtil.delete(sumImpOutputDir);
				}
				JobControl.SumCompanyJob(analyOutputDir, sumImpOutputDir, "imp");
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
				JobControl.AnalyCompanyJob(paths, analyOutputDir,companyID);
				if(CommonUtil.judgeDir(sumClkOutputDir)){
					CommonUtil.delete(sumClkOutputDir);
				}
				JobControl.SumCompanyJob(analyOutputDir, sumClkOutputDir, "clk");
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
				JobControl.CombineCompanyJob(paths, combineOutputDir,impOrClk);
				CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, combineOutputDir, "company",type );
				System.exit(0);
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
				JobControl.AnalyCompanyJob(paths, analyOutputDir,companyID);
				if(CommonUtil.judgeDir(sumImpOutputDir)){
					CommonUtil.delete(sumImpOutputDir);
				}
				JobControl.SumCompanyJob(analyOutputDir, sumImpOutputDir, impOrClk);
				//删除点击日志输出目录原有数据
				if(CommonUtil.judgeDir(sumClkOutputDir)){
					CommonUtil.delete(sumClkOutputDir);
				}
				//读取点击展现输出目录中的数据
				paths = CommonUtil.impAndClkLog(sumImpOutputDir, sumClkOutputDir);
				if(paths==null||paths.length==0){
					System.out.println("have no log");
					System.exit(0);
				}else{
					if(CommonUtil.judgeDir(combineOutputDir)){
						CommonUtil.delete(combineOutputDir);
					}
					JobControl.CombineCompanyJob(paths, combineOutputDir,impOrClk);
					CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, combineOutputDir, "company",type);
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
				JobControl.AnalyCompanyJob(paths, analyOutputDir,companyID);
				if(CommonUtil.judgeDir(sumClkOutputDir)){
					CommonUtil.delete(sumClkOutputDir);
				}
				JobControl.SumCompanyJob(analyOutputDir, sumClkOutputDir, impOrClk);
				//删除展现输出目录下的数据
				if(CommonUtil.judgeDir(sumImpOutputDir)){
					CommonUtil.delete(sumImpOutputDir);
				}
				//读取点击展现输出目录中的数据
				paths = CommonUtil.impAndClkLog(sumImpOutputDir, sumClkOutputDir);
				if(paths==null||paths.length==0){
					System.out.println("have no log");
					System.exit(0);
				}else{
					if(CommonUtil.judgeDir(combineOutputDir)){
						CommonUtil.delete(combineOutputDir);
					}
					JobControl.CombineCompanyJob(paths, combineOutputDir,impOrClk);
					CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, combineOutputDir, "company",type);
				}
				System.exit(0);
			}
		}
	}
*/}
