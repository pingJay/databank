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
*   
* 类名称：RunOrderAreaJob  
* 类描述：  分析活动地域报表
* 创建人：yang qi  
* 创建时间：2012-8-8 上午10:37:48  
* 修改人：yang qi  
* 修改时间：2012-8-8 上午10:37:48  
* 修改备注：  
* @version   
*
 */
public class RunOrderAreaJob implements RunCustomJob{
	private static final Logger logger = LoggerFactory.getLogger(RunOrderAreaJob.class);
	private JobControl jobControl;
	
	public RunOrderAreaJob(String queue) {
		jobControl = new JobControl(queue);
	}
	
	public boolean run(String beginDate,String endDate,String jobOptins,String resultnorm) {
		boolean isSuccessful = true;
		MapReduceJobOptionModel mrOptionModel = JobOptionsParse.parse(jobOptins);
		boolean impStatus = CommonUtil.beSureAnalyLogType(resultnorm, "imp");
		boolean clkStatus = CommonUtil.beSureAnalyLogType(resultnorm, "clk");
		String strDate = CommonUtil.strStartAndEndTime(beginDate, endDate);//返回日志时间
		String analyOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/AnalyCustomizeOrderArea/output";
		String sumImpOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/SumCustomizeOrderAreaImp/output";
		String sumClkOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/SumCustomizeOrderAreaClk/output";
		String combineOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/CombineCustomizeOrderArea/output";
		Path [] paths = null ;
		if(impStatus&&clkStatus){//分析展现日志和点击日志
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
				if(!jobControl.AnalyOrderAreaJob(paths, analyOutputDir  ,mrOptionModel.getOrder())){
					logger.error("RunOrderAreaJob AnalyOrderAreaJob imp proccess is faild");
					return false;
				}
				if(CommonUtil.judgeDir(sumImpOutputDir)){
					CommonUtil.delete(sumImpOutputDir);
				}
				if(!jobControl.SumOrderAreaJob(analyOutputDir, sumImpOutputDir, "imp")){
					logger.error("RunOrderAreaJob AnalyOrderAreaJob imp proccess is faild");
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
				if(!jobControl.AnalyOrderAreaJob(paths, analyOutputDir  ,mrOptionModel.getOrder())){
					logger.error("RunOrderAreaJob AnalyOrderAreaJob clk proccess is faild");
					return false;
				}
				if(CommonUtil.judgeDir(sumClkOutputDir)){
					CommonUtil.delete(sumClkOutputDir);
				}
				if(!jobControl.SumOrderAreaJob(analyOutputDir, sumClkOutputDir, "clk")){
					logger.error("RunOrderAreaJob SumOrderAreaJob clk proccess is faild");
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
				if(!jobControl.CombineOrderAreaJob(paths, combineOutputDir ,resultnorm)){
					logger.error("RunOrderAreaJob CombineOrderAreaJob  proccess is faild");
					return false;
				}
				//CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, combineOutputDir, "OrderArea",type);
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
				if(!jobControl.AnalyOrderAreaJob(paths, analyOutputDir  ,mrOptionModel.getOrder())){
					logger.error("RunOrderAreaJob AnalyOrderAreaJob imp proccess is faild");
					return false;
				}
				if(CommonUtil.judgeDir(sumImpOutputDir)){
					CommonUtil.delete(sumImpOutputDir);
				}
				if(!jobControl.SumOrderAreaJob(analyOutputDir, sumImpOutputDir, "imp")){
					logger.error("RunOrderAreaJob AnalyOrderAreaJob imp proccess is faild");
					return false;
				}
				//删除点击输出路径
				if(CommonUtil.judgeDir(sumClkOutputDir)){
					CommonUtil.delete(sumClkOutputDir);
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
					if(!jobControl.CombineOrderAreaJob(paths, combineOutputDir ,resultnorm)){
						logger.error("RunOrderAreaJob CombineOrderAreaJob  proccess is faild");
						return false;
					}
					//CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, combineOutputDir, "OrderArea",type);
				}
			}
		}else if(clkStatus){//点击日志
			paths = CommonUtil.clkLog(strDate);
			if(paths==null||paths.length==0){
				logger.warn("have no log for clk in " + strDate);
				return false;
			}else{
				if(CommonUtil.judgeDir(analyOutputDir)){
					CommonUtil.delete(analyOutputDir);
				}
				if(!jobControl.AnalyOrderAreaJob(paths, analyOutputDir  ,mrOptionModel.getOrder())){
					logger.error("RunOrderAreaJob AnalyOrderAreaJob clk proccess is faild");
					return false;
				}
				if(CommonUtil.judgeDir(sumClkOutputDir)){
					CommonUtil.delete(sumClkOutputDir);
				}
				if(!jobControl.SumOrderAreaJob(analyOutputDir, sumClkOutputDir, "clk")){
					logger.error("RunOrderAreaJob SumOrderAreaJob clk proccess is faild");
					return false;
				}
				//删除展现输出路径
				if(CommonUtil.judgeDir(sumImpOutputDir)){
					CommonUtil.delete(sumImpOutputDir);
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
					if(!jobControl.CombineOrderAreaJob(paths, combineOutputDir ,resultnorm)){
						logger.error("RunOrderAreaJob CombineOrderAreaJob  proccess is faild");
						return false;
					}
					//CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, combineOutputDir, "OrderArea",type);
				}
			}
		}
		return isSuccessful;
	}

/*	public static void main(String args[])throws Exception{
		String beginDate = null;
		String endDate  = null ; 
		String orderID = null;
		String queue = "queue3"; 
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
		String analyOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/AnalyCustomizeOrderArea/output";
		String sumImpOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/SumCustomizeOrderAreaImp/output";
		String sumClkOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/SumCustomizeOrderAreaClk/output";
		String combineOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/CombineCustomizeOrderArea/output";
		Path [] paths = null ;
		if(impStatus&&clkStatus){//分析展现日志和点击日志
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
				JobControl.AnalyOrderAreaJob(paths, analyOutputDir  ,orderID);
				if(CommonUtil.judgeDir(sumImpOutputDir)){
					CommonUtil.delete(sumImpOutputDir);
				}
				JobControl.SumOrderAreaJob(analyOutputDir, sumImpOutputDir, "imp");
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
				JobControl.AnalyOrderAreaJob(paths, analyOutputDir  ,orderID);
				if(CommonUtil.judgeDir(sumClkOutputDir)){
					CommonUtil.delete(sumClkOutputDir);
				}
				JobControl.SumOrderAreaJob(analyOutputDir, sumClkOutputDir, "clk");
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
				JobControl.CombineOrderAreaJob(paths, combineOutputDir ,impOrClk);
				CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, combineOutputDir, "OrderArea",type);
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
				JobControl.AnalyOrderAreaJob(paths, analyOutputDir  ,orderID);
				if(CommonUtil.judgeDir(sumImpOutputDir)){
					CommonUtil.delete(sumImpOutputDir);
				}
				JobControl.SumOrderAreaJob(analyOutputDir, sumImpOutputDir, "imp");
				//删除点击输出路径
				if(CommonUtil.judgeDir(sumClkOutputDir)){
					CommonUtil.delete(sumClkOutputDir);
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
					JobControl.CombineOrderAreaJob(paths, combineOutputDir ,impOrClk);
					CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, combineOutputDir, "OrderArea",type);
				}
				System.exit(0);
			}
		}else if(clkStatus){//点击日志
			paths = CommonUtil.clkLog(strDate);
			if(paths==null||paths.length==0){
				System.out.println("have no log");
				System.exit(0);
			}else{
				if(CommonUtil.judgeDir(analyOutputDir)){
					CommonUtil.delete(analyOutputDir);
				}
				JobControl.AnalyOrderAreaJob(paths, analyOutputDir  ,orderID);
				if(CommonUtil.judgeDir(sumClkOutputDir)){
					CommonUtil.delete(sumClkOutputDir);
				}
				JobControl.SumOrderAreaJob(analyOutputDir, sumClkOutputDir, "clk" );
				//删除展现输出路径
				if(CommonUtil.judgeDir(sumImpOutputDir)){
					CommonUtil.delete(sumImpOutputDir);
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
					JobControl.CombineOrderAreaJob(paths, combineOutputDir ,impOrClk);
					CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, combineOutputDir, "OrderArea",type);
				}
				System.exit(0);
			}
		}
	}
*/
}
