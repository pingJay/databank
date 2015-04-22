package com.uniclick.databank.mapreduce.job;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.jobconf_005fhistory_jsp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uniclick.databank.mapreduce.common.CommonUtil;
import com.uniclick.databank.mapreduce.job.iface.RunCustomJob;
import com.uniclick.databank.mapreduce.pojo.MapReduceJobOptionModel;
import com.uniclick.databank.mapreduce.util.JobOptionsParse;

/**
 * 
 * @ClassName: RunMediaHourJob
 * @Description: TODO(分析指定媒体小时数据)
 * @author ping.jie
 * @date 2015-4-14 上午11:21:10
 * @verision $Id
 *
 */
public class RunMediaHourJob implements RunCustomJob{
	private static final Logger logger = LoggerFactory.getLogger(RunMediaHourJob.class);
	private JobControl jobControl;
	
	public RunMediaHourJob(String queue) {
		jobControl = new JobControl(queue);
	}
	
	public boolean run(String beginDate,String endDate,String jobOptins,String resultnorm) {
		boolean isSuccessful = true;
		MapReduceJobOptionModel mrOptionModel = JobOptionsParse.parse(jobOptins);
		boolean impStatus = CommonUtil.beSureAnalyLogType(resultnorm, "imp");
		boolean clkStatus = CommonUtil.beSureAnalyLogType(resultnorm, "clk");
		String strDate = CommonUtil.strStartAndEndTime(beginDate, endDate);//返回日志时间
		String analyOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/AnalyCustomizeMediaHour/output";
		Path [] paths = null;
		if(impStatus){
			paths = CommonUtil.impLog(strDate);
			if(paths==null||paths.length==0){
				logger.warn("have no log for imp in " + strDate);
				return false;
			}else{
				if(CommonUtil.judgeDir(analyOutputDir)){
					CommonUtil.delete(analyOutputDir);
				}
				if(!jobControl.AnalyMediaHourJob(paths, analyOutputDir,mrOptionModel.getOrder())){
					logger.error("RunMediaHourJob AnalyMediaHourJob imp proccess is faild");
					return false;
				}
				//CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, analyOutputDir, "mediaHour", type);
			}
		}else if(clkStatus){
			paths = CommonUtil.clkLog(strDate);
			if(paths==null||paths.length==0){
				logger.warn("have no log for clk in " + strDate);
				return false;
			}else{
				if(CommonUtil.judgeDir(analyOutputDir)){
					CommonUtil.delete(analyOutputDir);
				}
				if(!jobControl.AnalyMediaHourJob(paths, analyOutputDir,mrOptionModel.getOrder())){
					logger.error("RunMediaHourJob AnalyMediaHourJob clk proccess is faild");
					return false;
				}
				//CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, analyOutputDir, "mediaHour", type);
			}
		}
		return isSuccessful;
	}
	
/*	public static void main(String args[])throws Exception{
		String beginDate = null;
		String endDate  = null ; 
		String queue = "queue3"; 
		String type = "customize";
		String impOrClk  = null;//区别分析点击日志或是展现日志
		String orderID = "0";
		if(args==null|| args.length==0){
		    System.out.println("[-B <beginTime>]" +
                    "[-E <endTime>] " +
                    "[-Q <queue>] "+
                    "[-IOC<imp or clk log>]"+
                    "[-OD<order>]");
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
		boolean impStatus = false;
		boolean clkStatus = true;
		impStatus = CommonUtil.beSureAnalyLogType(impOrClk, "imp");
		clkStatus = CommonUtil.beSureAnalyLogType(impOrClk, "clk");
		String strDate = CommonUtil.strStartAndEndTime(beginDate, endDate);//返回日志时间
		String analyOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/AnalyCustomizeMediaHour/output";
		Path [] paths = null;
		if(impStatus){
			paths = CommonUtil.impLog(strDate);
			if(paths==null||paths.length==0){
				System.out.println("have no log");
				System.exit(0);
			}else{
				if(CommonUtil.judgeDir(analyOutputDir)){
					CommonUtil.delete(analyOutputDir);
				}
				JobControl.AnalyMediaHourJob(paths, queue, analyOutputDir,orderID);
				CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, analyOutputDir, "mediaHour", type);
				System.exit(0);
			}
		}else if(clkStatus){
			paths = CommonUtil.clkLog(strDate);
			if(paths==null||paths.length==0){
				System.out.println("have no log");
				System.exit(0);
			}else{
				if(CommonUtil.judgeDir(analyOutputDir)){
					CommonUtil.delete(analyOutputDir);
				}
				JobControl.AnalyMediaHourJob(paths, queue, analyOutputDir, orderID);
				CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, analyOutputDir, "mediaHour", type);
				System.exit(0);
			}
		}
	}
*/
}
