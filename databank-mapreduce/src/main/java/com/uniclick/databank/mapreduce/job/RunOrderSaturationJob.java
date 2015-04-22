package com.uniclick.databank.mapreduce.job;

import java.text.SimpleDateFormat;
import java.util.Date;

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
* 类名称：RunOrderSaturationJob  
* 类描述：媒体饱和度  
* 创建人：yang qi  
* 创建时间：2012-8-14 下午02:08:16  
* 修改人：yang qi  
* 修改时间：2012-8-14 下午02:08:16  
* 修改备注：  
* @version   
*
 */
public class RunOrderSaturationJob implements RunCustomJob{
	private static final Logger logger = LoggerFactory.getLogger(RunOrderSaturationJob.class);
	private JobControl jobControl;
	
	public RunOrderSaturationJob(String queue) {
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
		String analyOutPutDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/AnalyseCenter/AnalyseSaturation/output";
		String sumOutPutDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/AnalyseCenter/SumSaturation/output";
		Path [] paths = null;
		if(impStatus){//读取展现日志
			paths = CommonUtil.impLog(strDate);
		}else if(clkStatus){
			paths = CommonUtil.clkLog(strDate);//读取点击日志
		}
		if(CommonUtil.judgeDir(analyOutPutDir)){//判断该输出路径是否存在,存在则删除
			CommonUtil.delete(analyOutPutDir);
		}
		
		if(!jobControl.AnalyseSaturationJob(paths, mrOptionModel.getOrder(), analyOutPutDir,endDate)){
			logger.error("RunOrderSaturationJob AnalyseSaturationJob imp proccess is faild");
			return false;
		}
		if(CommonUtil.judgeDir(sumOutPutDir)){//判断该输出路径是否存在,存在则删除
			CommonUtil.delete(sumOutPutDir);
		}
		if(!jobControl.SumSaturationJob(analyOutPutDir, sumOutPutDir,endDate)){
			logger.error("RunOrderSaturationJob SumSaturationJob imp proccess is faild");
			return false;
		}
		//CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, sumOutPutDir, localDir,"customize");//数据传递到本地
		return isSuccessful;
	}
	/**
	 * 
	 * @param args 传递计算饱和度的起始时间结束时间,以及广告活动ID和任务运行队列
	 * @throws Exception
	 */
/*	public static void main(String args[])throws Exception{
		//计算饱和度两个任务的输出路径
		//分析结果本地存放位置
		String localDir = "sitesaturation";
		String beginDate = null;
		String endDate  = null ; 
		String metrics = null;
		String queue = "queue1"; 
		String orderId = null;
		if(args.length<3||args==null){
		    System.out.println("[-B <beginTime>]" +
                    "[-E <endTime>] " +
                    "[-Q <queue>] "+
                    "[-OD <orderitemId>]"+
                    "[-IOC <metrics>]");
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
					metrics = args[++i];
				}
			}
		}
		boolean impStatus = false;
		boolean clkStatus = true;
		impStatus = CommonUtil.beSureAnalyLogType(metrics, "imp");
		clkStatus = CommonUtil.beSureAnalyLogType(metrics, "clk");
		String strDate = CommonUtil.strStartAndEndTime(beginDate, endDate);//返回日志时间
		String analyOutPutDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/AnalyseCenter/AnalyseSaturation/output";
		String sumOutPutDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/AnalyseCenter/SumSaturation/output";
		
		Path [] paths = null;
		if(impStatus){//读取展现日志
			paths = CommonUtil.impLog(strDate);
		}else if(clkStatus){
			paths = CommonUtil.clkLog(strDate);//读取点击日志
		}
		if(CommonUtil.judgeDir(analyOutPutDir)){//判断该输出路径是否存在,存在则删除
			CommonUtil.delete(analyOutPutDir);
		}
		
		JobControl.AnalyseSaturationJob(paths, orderId,queue, analyOutPutDir,endDate);//运行job
		if(CommonUtil.judgeDir(sumOutPutDir)){//判断该输出路径是否存在,存在则删除
			CommonUtil.delete(sumOutPutDir);
		}
		JobControl.SumSaturationJob(analyOutPutDir, queue, sumOutPutDir,endDate);//运行job
		CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, sumOutPutDir, localDir,"customize");//数据传递到本地
		
		System.exit(0);//退出job
	}
*/
}
