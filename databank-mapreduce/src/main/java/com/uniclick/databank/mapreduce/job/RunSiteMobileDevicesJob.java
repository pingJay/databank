package com.uniclick.databank.mapreduce.job;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uniclick.databank.mapreduce.common.CommonUtil;
import com.uniclick.databank.mapreduce.job.iface.RunCustomJob;
import com.uniclick.databank.mapreduce.pojo.MapReduceJobOptionModel;
import com.uniclick.databank.mapreduce.util.JobOptionsParse;


/**
 * 网站移动设备报告
 * @author zhou.yuanlong
 * @date 2013-12-17
 * @input 广告商id  网站id
 * @output 操作系统	设备型号	鼠标类型	浏览量	访问数	唯一用户数	跳出率	回访人数	平均停留时间	平均访问深度	每次访问页数
 */
public class RunSiteMobileDevicesJob implements RunCustomJob{
	private static final Logger logger = LoggerFactory.getLogger(RunSiteMobileDevicesJob.class);
	private JobControl jobControl;
	
	public RunSiteMobileDevicesJob(String queue) {
		jobControl = new JobControl(queue);
	}
	
	public boolean run(String beginDate,String endDate,String jobOptins,String resultnorm) {
		boolean isSuccessful = true;
		MapReduceJobOptionModel mrOptionModel = JobOptionsParse.parse(jobOptins);
		String strDate = CommonUtil.strStartAndEndTime(beginDate, endDate);//返回日志时间
		String analySiteMobileDevicesUAOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/AnalySiteMobileDevicesUA/output";
		String analyOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/AnalySiteMobileDevices/output";
		String sumImpOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/SumSiteMobileDevices/output";
		Path [] paths = CommonUtil.siteLog(strDate) ;
		if(paths==null||paths.length==0){
			logger.warn("have no log for site in " + strDate);
			return false;
		}else{
			//分析 操作系统	设备型号	鼠标类型
			if(CommonUtil.judgeDir(analySiteMobileDevicesUAOutputDir)){
				CommonUtil.delete(analySiteMobileDevicesUAOutputDir);
			}
			if(!jobControl.AnalySiteMobileDevicesUAJob(paths, analySiteMobileDevicesUAOutputDir,  mrOptionModel.getSite(), "mobileDevice")){
				logger.error("RunSiteMobileDevicesJob AnalySiteMobileDevicesUAJob proccess is faild");
				return false;
			}
			
			
			if(CommonUtil.judgeDir(analyOutputDir)){
				CommonUtil.delete(analyOutputDir);
			}
			if(!jobControl.AnalySiteMobileDevicesJob(analySiteMobileDevicesUAOutputDir, analyOutputDir)){
				logger.error("RunSiteMobileDevicesJob AnalySiteMobileDevicesJob proccess is faild");
				return false;
			}
			
			if(CommonUtil.judgeDir(sumImpOutputDir)){
				CommonUtil.delete(sumImpOutputDir);
			}
			if(!jobControl.SumSiteMobileDevicesJob(analyOutputDir, sumImpOutputDir)){
				logger.error("RunSiteMobileDevicesJob SumSiteMobileDevicesJob proccess is faild");
				return false;
			}
			
			//CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, sumImpOutputDir, "SiteMobileDevices",type);
		}
		return isSuccessful;
	}
/*	public static void main(String args[])throws Exception{
		String beginDate = null;
		String endDate  = null ;
		String site  = null ; 
		String queue = "queue3"; 
		String type = "customize";
		if(args==null|| args.length==0){//不传日期参数的话，默认只分析昨天的日志
			beginDate = CommonUtil.yesterdayDate();
			endDate = CommonUtil.yesterdayDate();
		}else{
			for(int i=0 ;i<args.length ; i++){
				if("-B".equals(args[i])){
					beginDate = args[++i];
				}else if("-E".equals(args[i])){
					endDate = args[++i];
				}else if("-SID".equals(args[i])){
					site = args[++i];//网站可以有多个
				}else if("-Q".equals(args[i])){
					queue = args[++i];
				}
			}
		}
		String strDate = CommonUtil.strStartAndEndTime(beginDate, endDate);//返回日志时间
		String analySiteMobileDevicesUAOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/AnalySiteMobileDevicesUA/output";
		String analyOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/AnalySiteMobileDevices/output";
		String sumImpOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/SumSiteMobileDevices/output";
		Path [] paths = CommonUtil.siteLog(strDate) ;
		if(paths==null||paths.length==0){
			System.out.println("have no log");
			System.exit(0);
		}else{
			//分析 操作系统	设备型号	鼠标类型
			if(CommonUtil.judgeDir(analySiteMobileDevicesUAOutputDir)){
				CommonUtil.delete(analySiteMobileDevicesUAOutputDir);
			}
			JobControl.AnalySiteMobileDevicesUAJob(paths, analySiteMobileDevicesUAOutputDir,  site, "mobileDevice");
			
			
			if(CommonUtil.judgeDir(analyOutputDir)){
				CommonUtil.delete(analyOutputDir);
			}
			JobControl.AnalySiteMobileDevicesJob(analySiteMobileDevicesUAOutputDir, analyOutputDir);
			
			if(CommonUtil.judgeDir(sumImpOutputDir)){
				CommonUtil.delete(sumImpOutputDir);
			}
			JobControl.SumSiteMobileDevicesJob(analyOutputDir, sumImpOutputDir);
			
			CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, sumImpOutputDir, "SiteMobileDevices",type);
			System.exit(0);
			
		}
	}
*/
}
