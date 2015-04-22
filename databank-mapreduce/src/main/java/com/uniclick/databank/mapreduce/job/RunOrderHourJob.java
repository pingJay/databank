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
 * @ClassName: RunOrderHourJob
 * @Description: TODO(分析指定活动小时数据)
 * @author ping.jie
 * @date 2015-4-14 下午12:13:12
 * @verision $Id
 *
 */
public class RunOrderHourJob implements RunCustomJob{
	private static final Logger logger = LoggerFactory.getLogger(RunOrderHourJob.class);
	private JobControl jobControl;
	
	public RunOrderHourJob(String queue) {
		jobControl = new JobControl(queue);
	}
	
	public boolean run(String beginDate,String endDate,String jobOptins,String resultnorm) {
		boolean isSuccessful = true;
		MapReduceJobOptionModel mrOptionModel = JobOptionsParse.parse(jobOptins);
		boolean impStatus = CommonUtil.beSureAnalyLogType(resultnorm, "imp");
		boolean clkStatus = CommonUtil.beSureAnalyLogType(resultnorm, "clk");
		String strDate = CommonUtil.strStartAndEndTime(beginDate, endDate);//返回日志时间
		String analyOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/AnalyCustomizeOrderHour/output";
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
				if(!jobControl.AnalyOrderHourJob(paths, analyOutputDir, mrOptionModel.getCompany(),mrOptionModel.getOrder())){
					logger.error("RunOrderHourJob AnalyOrderHourJob imp proccess is faild");
					return false;
				}
				//CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, analyOutputDir, "orderHourPv", type);
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
				if(!jobControl.AnalyOrderHourJob(paths, analyOutputDir, mrOptionModel.getCompany(),mrOptionModel.getOrder())){
					logger.error("RunOrderHourJob AnalyOrderHourJob clk proccess is faild");
					return false;
				}
				//CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, analyOutputDir, "orderHourClk", type);
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
		String companyID = "0";
		String orderID = "0";
		if(args==null|| args.length==0){
		    System.out.println("[-B <beginTime>]" +
                    "[-E <endTime>] " +
                    "[-Q <queue>] "+
                    "[-IOC<imp or clk log>]"+
                    "[-OrderID<order>]" +
                    "[-CompanyID <compantId> ]" );
		    System.exit(0);
		}else{
			for(int i=0 ;i<args.length ; i++){
				if("-B".equals(args[i])){
					beginDate = args[++i];
				}else if("-E".equals(args[i])){
					endDate = args[++i];
				}else if("-Q".equals(args[i])){
					queue = args[++i];
				}else if("-CompanyID".equals(args[i])){
					companyID = args[++i];
				}else if("-IOC".equals(args[i])){
					impOrClk = args[++i];
				}else if("-OrderID".equals(args[i])){
					orderID = args[++i];
				}
			}
		}
		String strDate = CommonUtil.strStartAndEndTime(beginDate, endDate);//返回日志时间
		String analyOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/AnalyCustomizeOrderHour/output";
		Path [] paths = null;
		if("imp".equals(impOrClk)){
			paths = CommonUtil.impLog(strDate);
			if(paths==null||paths.length==0){
				System.out.println("have no log");
				System.exit(0);
			}else{
				if(CommonUtil.judgeDir(analyOutputDir)){
					CommonUtil.delete(analyOutputDir);
				}
				JobControl.AnalyOrderHourJob(paths, queue, analyOutputDir, companyID,orderID);
				CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, analyOutputDir, "orderHourPv", type);
				System.exit(0);
			}
		}else if("clk".equals(impOrClk)){
			paths = CommonUtil.clkLog(strDate);
			if(paths==null||paths.length==0){
				System.out.println("have no log");
				System.exit(0);
			}else{
				if(CommonUtil.judgeDir(analyOutputDir)){
					CommonUtil.delete(analyOutputDir);
				}
				JobControl.AnalyOrderHourJob(paths, queue, analyOutputDir, companyID,orderID);
				CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, analyOutputDir, "orderHourClk", type);
				System.exit(0);
			}
		}
	}
*/
}
