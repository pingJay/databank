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
 * @ClassName: RunActRegionJob
 * @Description: TODO(分析指定页面地域数据)
 * @author ping.jie
 * @date 2015-4-13 下午3:50:46
 * @verision V2.0.0
 *
 */
public class RunActRegionJob implements RunCustomJob{
	private static final Logger logger = LoggerFactory.getLogger(RunActRegionJob.class);
	private JobControl jobControl;
	
	public RunActRegionJob(String queue) {
		jobControl = new JobControl(queue);
	}
	
	public boolean run(String beginDate,String endDate,String jobOptins,String resultnorm) {
		boolean isSuccessful = true;
		MapReduceJobOptionModel mrOptionModel = JobOptionsParse.parse(jobOptins);
		String strDate = CommonUtil.strStartAndEndTime(beginDate, endDate);//返回日志时间
		String analyOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/AnalyCustomizeActRegion/output";
		String sumImpOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/SumCustomizeActRegion/output";
		Path [] paths = CommonUtil.pageVisitLog(strDate) ;
		if(paths==null||paths.length==0){
			logger.warn("have no log for acy in " + strDate);
			return false;
		}else{
			if(CommonUtil.judgeDir(analyOutputDir)){
				CommonUtil.delete(analyOutputDir);
			}
			if(!jobControl.AnalyActRegionJob(paths, analyOutputDir,mrOptionModel.getAct())){
				logger.error("RunActRegionJob AnalyActRegionJob proccess is faild");
				return false;
			}
			if(CommonUtil.judgeDir(sumImpOutputDir)){
				CommonUtil.delete(sumImpOutputDir);
			}
			if(!jobControl.SumActRegionJob(analyOutputDir, sumImpOutputDir,resultnorm)){
				logger.error("RunActRegionJob SumActRegionJob proccess is faild");
				return false;
			}
			//CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, sumImpOutputDir, "actRegion",type);
		}
		return isSuccessful;
	}
	
/*	public static void main(String args[])throws Exception{
		String beginDate = null;
		String endDate  = null ; 
		String queue = "queue3"; 
		String metrics = null;
		String type = "customize";
		String actID = "0";
		if(args==null|| args.length==0){
		    System.out.println("[-B <beginTime>]" +
                    "[-E <endTime>] " +
                    "[-Q <queue>] "+
                    "[-ActID <actID>]" );
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
				}else if("-IOC".equals(args[i])){
					metrics = args[++i];
				}
			}
		}
		String strDate = CommonUtil.strStartAndEndTime(beginDate, endDate);//返回日志时间
		String analyOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/AnalyCustomizeActRegion/output";
		String sumImpOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/SumCustomizeActRegion/output";
		Path [] paths = CommonUtil.pageVisitLog(strDate) ;
		if(paths==null||paths.length==0){
			System.out.println("have no log");
			System.exit(0);
		}else{
			if(CommonUtil.judgeDir(analyOutputDir)){
				CommonUtil.delete(analyOutputDir);
			}
			JobControl.AnalyActRegionJob(paths, queue, analyOutputDir,actID);
			if(CommonUtil.judgeDir(sumImpOutputDir)){
				CommonUtil.delete(sumImpOutputDir);
			}
			JobControl.SumActRegionJob(analyOutputDir, queue, sumImpOutputDir,metrics);
			CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, sumImpOutputDir, "actRegion",type);
			System.exit(0);
		}
	}
*/
}
