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
* 类名称：RunOrderSiteCoincideJob  
* 类描述： 分析指定活动下的媒体重新度
* 创建人：yang qi  
* 创建时间：2012-8-8 下午03:12:40  
* 修改人：yang qi  
* 修改时间：2012-8-8 下午03:12:40  
* 修改备注：  
* @version   
*
 */
public class RunOrderSiteCoincideJob implements RunCustomJob{
	private static final Logger logger = LoggerFactory.getLogger(RunOrderSiteCoincideJob.class);
	private JobControl jobControl;
	
	public RunOrderSiteCoincideJob(String queue) {
		jobControl = new JobControl(queue);
	}
	
	public boolean run(String beginDate,String endDate,String jobOptins,String resultnorm) {
		boolean isSuccessful = true;
		MapReduceJobOptionModel mrOptionModel = JobOptionsParse.parse(jobOptins);
		boolean impStatus = CommonUtil.beSureAnalyLogType(resultnorm, "imp");
		boolean clkStatus = CommonUtil.beSureAnalyLogType(resultnorm, "clk");
		String strDate = CommonUtil.strStartAndEndTime(beginDate, endDate);//返回日志时间
		String analyOutPutDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/AnalyseOrderSiteCoincide/output";
		String sumOutPutDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/SumOrderSiteCoincide/output";
		Path [] paths = null ;
		if(impStatus){//读取展现日志
			paths = CommonUtil.impLog(strDate);
		}else if(clkStatus){
			paths = CommonUtil.clkLog(strDate);//读取点击日志
		}
		if(paths.length==0){
			System.out.println("have no log");
			System.exit(0);
		}else{
			if(CommonUtil.judgeDir(analyOutPutDir)){
				CommonUtil.delete(analyOutPutDir);
			}
			if(!jobControl.AnalyOrderSiteCoincide(analyOutPutDir, paths, mrOptionModel.getOrder() , mrOptionModel.getCoincide())){
				logger.error("RunOrderSiteCoincideJob AnalyOrderSiteCoincide  proccess is faild");
				return false;
			}
			if(CommonUtil.judgeDir(sumOutPutDir)){
				CommonUtil.delete(sumOutPutDir);
			}
			if(!jobControl.SumOrderSiteCoincide(analyOutPutDir, sumOutPutDir)){
				logger.error("RunOrderSiteCoincideJob SumOrderSiteCoincide  proccess is faild");
				return false;
			}
			//CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, sumOutPutDir, "mediaCoicide",type);
		}
		return isSuccessful;
	}
/*	public static void main(String args[])throws Exception{
		//分析结果本地存放位置
		String beginDate = null;
		String endDate  = null ; 
		String queue = "queue1"; 
		String orderId = null;
		String impOrClk  = null;//区别分析点击日志或是展现日志
		String coincide = null;
		String type = "customize";
		if(args.length<4||args==null){
		    System.out.println("[-B <beginTime>]" +
                    "[-E <endTime>] " +
                    "[-Q <queue>] "+
                    "[-IOC<clk_log or imp_log>]"+
                    "[-Coincide <coincide>]"+
		    		"[-OD <orderId>]");
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
				}else if("-Coincide".equals(args[i])){
					coincide = args[++i];
				}
			}
		}
		String strDate = CommonUtil.strStartAndEndTime(beginDate, endDate);//返回日志时间
		String analyOutPutDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/AnalyseOrderSiteCoincide/output";
		String sumOutPutDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/SumOrderSiteCoincide/output";
		Path [] paths = null ;
		if("uv".equals(impOrClk)){//读取展现日志
			paths = CommonUtil.impLog(strDate);
		}else if("uc".equals(impOrClk)){
			paths = CommonUtil.clkLog(strDate);//读取点击日志
		}
		if(paths.length==0){
			System.out.println("have no log");
			System.exit(0);
		}else{
			if(CommonUtil.judgeDir(analyOutPutDir)){
				CommonUtil.delete(analyOutPutDir);
			}
			JobControl.AnalyOrderSiteCoincide(analyOutPutDir, queue, paths, orderId , coincide);
			if(CommonUtil.judgeDir(sumOutPutDir)){
				CommonUtil.delete(sumOutPutDir);
			}
			JobControl.SumOrderSiteCoincide(analyOutPutDir, queue, sumOutPutDir);
			CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, sumOutPutDir, "mediaCoicide",type);
			System.exit(0);
		}
	}
*/
}
