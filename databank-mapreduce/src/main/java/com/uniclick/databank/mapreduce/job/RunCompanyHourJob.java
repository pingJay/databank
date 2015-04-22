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
 * @ClassName: RunCompanyHourJob
 * @Description: TODO(分析指定厂商小时数据)
 * @author ping.jie
 * @date 2015-4-14 上午10:33:36
 * @verision 2.0.0
 *
 */
public class RunCompanyHourJob implements RunCustomJob{
	private static final Logger logger = LoggerFactory.getLogger(RunCompanyHourJob.class);
	private JobControl jobControl;
	
	public RunCompanyHourJob(String queue) {
		jobControl = new JobControl(queue);
	}
	
	public boolean run(String beginDate,String endDate,String jobOptins,String resultnorm) {
		boolean isSuccessful = true;
		MapReduceJobOptionModel mrOptionModel = JobOptionsParse.parse(jobOptins);
		boolean impStatus = CommonUtil.beSureAnalyLogType(resultnorm, "imp");
		boolean clkStatus = CommonUtil.beSureAnalyLogType(resultnorm, "clk");
		String strDate = CommonUtil.strStartAndEndTime(beginDate, endDate);//返回日志时间
		String analyOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/AnalyCustomizeAdvHour/output";
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
				if(!jobControl.AnalyComapnyHourJob(paths, analyOutputDir, mrOptionModel.getCompany())){
					logger.error("RunCompanyHourJob AnalyComapnyHourJob imp proccess is faild");
					return false;
				}
				//CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, analyOutputDir, "companyHourPv", type);
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
				if(!jobControl.AnalyComapnyHourJob(paths, analyOutputDir, mrOptionModel.getCompany())){
					logger.error("RunCompanyHourJob AnalyComapnyHourJob clk proccess is faild");
					return false;
				}
				//CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, analyOutputDir, "companyHourClk", type);
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
		if(args==null|| args.length==0){
		    System.out.println("[-B <beginTime>]" +
                    "[-E <endTime>] " +
                    "[-Q <queue>] "+
                    "[-IOC<imp or clk log>]"+
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
				}
			}
		}
		String strDate = CommonUtil.strStartAndEndTime(beginDate, endDate);//返回日志时间
		String analyOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/AnalyCustomizeAdvHour/output";
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
				JobControl.AnalyComapnyHourJob(paths, queue, analyOutputDir, companyID);
				CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, analyOutputDir, "companyHourPv", type);
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
				JobControl.AnalyComapnyHourJob(paths, queue, analyOutputDir, companyID);
				CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, analyOutputDir, "companyHourClk", type);
				System.exit(0);
			}
		}
	}
*/
}
