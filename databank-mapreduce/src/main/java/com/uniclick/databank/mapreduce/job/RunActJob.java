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
 * @ClassName: RunActJob
 * @Description: TODO(分析指定页面数据)
 * @author ping.jie
 * @date 2015-4-13 下午3:27:05
 * @verision $Id
 *
 */
public class RunActJob implements RunCustomJob{
	private static final Logger logger = LoggerFactory.getLogger(RunActJob.class);
	private JobControl jobControl;
	
	public RunActJob(String queue) {
		jobControl = new JobControl(queue);
	}
	
	public boolean run(String beginDate,String endDate,String jobOptins,String resultnorm) {
		boolean isSuccessful = true;
		MapReduceJobOptionModel mrOptionModel = JobOptionsParse.parse(jobOptins);
		String strDate = CommonUtil.strStartAndEndTime(beginDate, endDate);//返回日志时间
		String analyOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/AnalyCustomizeAct/output";
		String sumImpOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/SumCustomizeAct/output";
		Path [] paths = CommonUtil.pageVisitLog(strDate) ;
		if(paths==null||paths.length==0){
			logger.warn("have no log for acy in " + strDate);
			return false;
		}else{
			if(CommonUtil.judgeDir(analyOutputDir)){
				CommonUtil.delete(analyOutputDir);
			}
			if(!jobControl.AnalyActJob(paths, analyOutputDir,mrOptionModel.getAct())){
				logger.error("RunActJob AnalyActJob proccess is faild");
				return false;
			}
			if(CommonUtil.judgeDir(sumImpOutputDir)){
				CommonUtil.delete(sumImpOutputDir);
			}
			if(!jobControl.SumActJob(analyOutputDir, sumImpOutputDir , resultnorm)){
				logger.error("RunActJob SumActJob proccess is faild");
				return false;
			}
			//CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, sumImpOutputDir, "act",type);
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
                    "[-IOC <metrics>]"+
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
		String analyOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/AnalyCustomizeAct/output";
		String sumImpOutputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/SumCustomizeAct/output";
		Path [] paths = CommonUtil.pageVisitLog(strDate) ;
		if(paths==null||paths.length==0){
			System.out.println("have no log");
			System.exit(0);
		}else{
			if(CommonUtil.judgeDir(analyOutputDir)){
				CommonUtil.delete(analyOutputDir);
			}
			JobControl.AnalyActJob(paths, queue, analyOutputDir,actID);
			if(CommonUtil.judgeDir(sumImpOutputDir)){
				CommonUtil.delete(sumImpOutputDir);
			}
			JobControl.SumActJob(analyOutputDir, queue, sumImpOutputDir , metrics);
			CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, sumImpOutputDir, "act",type);
			System.exit(0);
		}
	}
*/
}
