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
* 类名称：RunOrderOrderItemRefreralJob  
* 类描述：分析指定活动下的广告来源数据量  
* 创建人：yang qi  
* 创建时间：2012-8-14 上午11:04:35  
* 修改人：yang qi  
* 修改时间：2012-8-14 上午11:04:35  
* 修改备注：
* 第二次修改人：zhou yuanlong
* 第二次修改时间：2013-07-05
* 修改备注：增加包含网页完整地址功能
* @version   
*
 */
public class RunOrderOrderItemRefreralJob implements RunCustomJob{
	private static final Logger logger = LoggerFactory.getLogger(RunOrderOrderItemRefreralJob.class);
	private JobControl jobControl;
	
	public RunOrderOrderItemRefreralJob(String queue) {
		jobControl = new JobControl(queue);
	}
	
	public boolean run(String beginDate,String endDate,String jobOptins,String resultnorm) {
		boolean isSuccessful = true;
		MapReduceJobOptionModel mrOptionModel = JobOptionsParse.parse(jobOptins);
		boolean impStatus = false;
		boolean clkStatus = true;
		impStatus = CommonUtil.beSureAnalyLogType(resultnorm, "imp");
		clkStatus = CommonUtil.beSureAnalyLogType(resultnorm, "clk");
		String strDate = CommonUtil.strStartAndEndTime(beginDate, endDate);//返回日志时间
		String outputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/OrderOrderItemRefreral/output";//任务输出路径
		Path [] paths = null;
		if(impStatus){//读取展现日志
			paths = CommonUtil.impLog(strDate);
		}else if(clkStatus){
			paths = CommonUtil.clkLog(strDate);//读取点击日志
		}
		if(paths==null || paths.length==0){
			logger.warn("have no log for imp in " + strDate);
			return false;
		}else{
			if(CommonUtil.judgeDir(outputDir)){
				CommonUtil.delete(outputDir);
			}
			if(!jobControl.AnalyOrderItemReferralPv(outputDir, paths ,mrOptionModel.getOrder(), mrOptionModel.getUrltype())){
				logger.error("RunOrderOrderItemRefreralJob AnalyOrderItemReferralPv imp proccess is faild");
				return false;
			}
			//String localDir = "OrderItemReferral";//任务本地输出名称
			//CommonUtil.ftpToLocal((beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate), outputDir, localDir , "customize");
		}
		return isSuccessful;
	}
	
/*	public static void main(String args[])throws Exception{
		String beginDate = null;//任务起始时间
		String endDate  = null ; //任务结束时间
		String queue = "queue1"; //队列
		String orderId = null;//活动ID
		String impOrClk  = null;//区别分析点击日志或是展现日志
		String urlType  = null;//区别是域名形式（1）还是包含网页完整地址（2） 
		if(args.length<4||args==null){
		    System.out.println("[-B <beginTime>]" +
                    "[-E <endTime>] " +
                    "[-Q <queue>] "+
                    "[-IOC<clk_log or imp_log>]"+
                    "[-URLTYPE<urltype>]"+
                    "[-OD <orderitemId>]");
		    System.exit(0);
		}else{
			for(int i=0 ;i<args.length ; i++){
				if("-B".equals(args[i])){
					beginDate = args[++i];
				}else if("-E".equals(args[i])){
					endDate = args[++i];
				}else if("-Q".equals(args[i])){
					queue = args[++i];
				}else if("-OD".equals(args[i])){
					orderId = args[++i];
				}else if("-IOC".equals(args[i])){
					impOrClk = args[++i];
				}else if("-URLTYPE".equals(args[i])){
					urlType = args[++i];
				}
			}
		}
		boolean impStatus = false;
		boolean clkStatus = true;
		impStatus = CommonUtil.beSureAnalyLogType(impOrClk, "imp");
		clkStatus = CommonUtil.beSureAnalyLogType(impOrClk, "clk");
		String strDate = CommonUtil.strStartAndEndTime(beginDate, endDate);//返回日志时间
		String outputDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/OrderOrderItemRefreral/output";//任务输出路径
		Path [] paths = null;
		if(impStatus){//读取展现日志
			paths = CommonUtil.impLog(strDate);
		}else if(clkStatus){
			paths = CommonUtil.clkLog(strDate);//读取点击日志
		}
		if(paths==null || paths.length==0){
			System.out.println("have no log");
			System.exit(0);
		}else{
			if(CommonUtil.judgeDir(outputDir)){
				CommonUtil.delete(outputDir);
			}
			JobControl.AnalyOrderItemReferralPv(outputDir, queue, paths ,orderId, urlType);
			String localDir = "OrderItemReferral";//任务本地输出名称
			CommonUtil.ftpToLocal((beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate), outputDir, localDir , "customize");
		}
	}
*/
}
