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
 * @date 2012-06-26
 * @version 1.0
 * @function 分析指定页面数据
 */
public class RunSiteCrowdJob implements RunCustomJob{
	private static final Logger logger = LoggerFactory.getLogger(RunSiteCrowdJob.class);
	private JobControl jobControl;
	
	public RunSiteCrowdJob(String queue) {
		jobControl = new JobControl(queue);
	}
	
	public boolean run(String beginDate,String endDate,String jobOptins,String resultnorm) {
		boolean isSuccessful = true;
		MapReduceJobOptionModel mrOptionModel = JobOptionsParse.parse(jobOptins);
		String strDate = CommonUtil.strStartAndEndTime(beginDate, endDate);//返回日志时间
		String analyOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/AnalySiteCrowd/output";
		String sumOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/sumSiteCorwd/output";
		Path [] paths = CommonUtil.siteLog(strDate) ;
		if(paths==null||paths.length==0){
			logger.warn("have no log for site in " + strDate);
			return false;
		}else{
			if(CommonUtil.judgeDir(analyOutputDir)){
				CommonUtil.delete(analyOutputDir);
			}
			if(!jobControl.AnalySiteCrowdJob(paths, analyOutputDir,mrOptionModel.getSite())){
				logger.error("RunSiteCrowdJob AnalySiteCrowdJob proccess is faild");
				return false;
			}
			if(CommonUtil.judgeDir(sumOutputDir)){
				CommonUtil.delete(sumOutputDir);
			}
			if(!jobControl.sumSiteCrowdJob(analyOutputDir, sumOutputDir,resultnorm)){
				logger.error("RunSiteCrowdJob sumSiteCrowdJob proccess is faild");
				return false;
			}
			//CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, sumOutputDir, "SiteCrowd",type);
		}
		return isSuccessful;
	}
/*	public static void main(String args[])throws Exception{
		String beginDate = null;
		String endDate  = null ; 
		String queue = "queue3"; 
		String type = "customize";
		String siteId = null;
		String metrics = null;
		if(args==null|| args.length==0){
		    System.out.println("[-B <beginTime>]" +
                    "[-E <endTime>] " +
                    "[-Q <queue>] "+
                    "[-IOC<metrics>]"+
                    "[-SID <siteId>]" );
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
					metrics = args[++i];
				}else if("-SID".equals(args[i])){
					siteId = args[++i];
				}
			}
		}
		String strDate = CommonUtil.strStartAndEndTime(beginDate, endDate);//返回日志时间
		String analyOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/AnalySiteCrowd/output";
		String sumOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/sumSiteCorwd/output";
		Path [] paths = CommonUtil.siteLog(strDate) ;
		if(paths==null||paths.length==0){
			System.out.println("have no log");
			System.exit(0);
		}else{
			if(CommonUtil.judgeDir(analyOutputDir)){
				CommonUtil.delete(analyOutputDir);
			}
			JobControl.AnalySiteCrowdJob(paths, queue, analyOutputDir,siteId);
			if(CommonUtil.judgeDir(sumOutputDir)){
				CommonUtil.delete(sumOutputDir);
			}
			JobControl.sumSiteCrowdJob(analyOutputDir, queue, sumOutputDir,metrics);
			CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, sumOutputDir, "SiteCrowd",type);
			System.exit(0);
		}
	}
*/
}
