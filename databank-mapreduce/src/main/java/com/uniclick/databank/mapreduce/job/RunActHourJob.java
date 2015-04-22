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
 * @ClassName: RunActHourJob
 * @Description: TODO(指定页面小时报)
 * @author ping.jie
 * @date 2015-4-13 下午3:22:47
 * @verision $Id
 *
 */
public class RunActHourJob implements RunCustomJob{
	private static final Logger logger = LoggerFactory.getLogger(RunActHourJob.class);
	private JobControl jobControl;
	
	public RunActHourJob(String queue) {
		jobControl = new JobControl(queue);
	}
	
	public boolean run(String beginDate,String endDate,String jobOptins,String resultnorm) {
		boolean isSuccessful = true;
		MapReduceJobOptionModel mrOptionModel = JobOptionsParse.parse(jobOptins);
		String strDate = CommonUtil.strStartAndEndTime(beginDate, endDate);//返回日志时间
		String analyOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/AnalyCustomizeActHour/output";
		Path [] paths = CommonUtil.acyLog(strDate);
		if(paths==null||paths.length==0){
			logger.warn("have no log for acy in " + strDate);
			return false;
		}else{
			if(CommonUtil.judgeDir(analyOutputDir)){
				CommonUtil.delete(analyOutputDir);
			}
			if(!jobControl.AnalyActHourJob(paths, analyOutputDir, mrOptionModel.getAct())){
				logger.error("RunActHourJob AnalyActHourJob proccess is faild");
				return false;
			}
			//CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, analyOutputDir, "actHour", type);
		}
		return isSuccessful;
	}
/*	public static void main(String args[])throws Exception{
		String beginDate = null;
		String endDate  = null ; 
		String queue = "queue3"; 
		String type = "customize";
		String actID = "0";
		if(args==null|| args.length==0){
		    System.out.println("[-B <beginTime>]" +
                    "[-E <endTime>] " +
                    "[-Q <queue>] "+
                    "[-IOC<imp or clk log>]"+
                    "[-ActID <actid> ]" );
		    System.exit(0);
		}else{
			for(int i=0 ;i<args.length ; i++){
				if("-B".equals(args[i])){
					beginDate = args[++i];
				}else if("-E".equals(args[i])){
					endDate = args[++i];
				}else if("-Q".equals(args[i])){
					queue = args[++i];
				}else if("-ActID".equals(args[i])){
					actID = args[++i];
				}
			}
		}
		String strDate = CommonUtil.strStartAndEndTime(beginDate, endDate);//返回日志时间
		String analyOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/AnalyCustomizeActHour/output";
		Path [] paths = CommonUtil.acyLog(strDate);
		if(paths==null||paths.length==0){
			System.out.println("have no log");
			System.exit(0);
		}else{
			if(CommonUtil.judgeDir(analyOutputDir)){
				CommonUtil.delete(analyOutputDir);
			}
			JobControl.AnalyActHourJob(paths, queue, analyOutputDir, actID);
			CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, analyOutputDir, "actHour", type);
			System.exit(0);
		}
	}
*/
}
