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
* 类名称：RunAnalyInventoryFrequencyPvUvClkUcMergeJob  
* 类描述：分析指定活动媒体广告位频次PV UV CLK UV数据合并  
* 创建人：zhou.yuanlong
* 创建时间：2013-08-16
* @version   
*
 */
public class RunAnalyInventoryFrequencyJob implements RunCustomJob{

	private static final Logger logger = LoggerFactory.getLogger(RunAnalyInventoryFrequencyJob.class);
	private JobControl jobControl;
	
	public RunAnalyInventoryFrequencyJob(String queue) {
		jobControl = new JobControl(queue);
	}
	
	/**
	 * 
	 * @Title: run
	 * @Description: TODO()
	 * @param @param beginDate 开始时间
	 * @param @param endDate   结束时间
	 * @param @param jobOptins	JOB接受的参数，json格式
	 * @param @param resultnorm 生成结果的列名
	 * @param @return    设定文件
	 * @return boolean    返回类型
	 * @throws
	 */
	public boolean run(String beginDate,String endDate,String jobOptins,String resultnorm) {
		boolean isSuccessful = true;
		boolean impStatus = true;
		boolean clkStatus = true;
		MapReduceJobOptionModel jobOptionModel = JobOptionsParse.parse(jobOptins);
		String strDate = CommonUtil.strStartAndEndTime(beginDate, endDate);//返回日志时间
		String analyOutPutDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/AnalyInventoryFrequencyPvUvClkUc/output";
		String sumPvUvOutPutDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/SumInventoryFrequencyPvUv/output";
		String sumClkUcOutPutDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/SumInventoryFrequencyClkUc/output";
		String sumPvUvClkUcMergeOutPutDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/SumInventoryFrequencyPvUvClkUcMerge/output";
		String sortPvUvClkUcMergeOutPutDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/SortInventoryFrequencyPvUvClkUcMerge/output";
		Path [] paths = null ;
		
		//分析展现日志
		if(impStatus){//读取展现日志
			paths = CommonUtil.impLog(strDate);
		}
		if(paths==null||paths.length==0){
			logger.warn("have no log for imp in " + strDate);
			return false;
		}else{
			if(CommonUtil.judgeDir(analyOutPutDir)){
				CommonUtil.delete(analyOutPutDir);
			}
			
			if(!jobControl.AnalyInventoryFrequencyPVUV(analyOutPutDir, paths, jobOptionModel.getOrder(), 
					jobOptionModel.getSite(), jobOptionModel.getInventory(), jobOptionModel.getCollect())){
				logger.error("RunAnalyInventoryFrequencyJob AnalyInventoryFrequencyPVUV imp proccess is faild");
				return false;
			}
			if(CommonUtil.judgeDir(sumPvUvOutPutDir)){
				CommonUtil.delete(sumPvUvOutPutDir);
			}
			//分析结果本地存放位置
			if(!jobControl.SumInventoryFrequencyPVUV(analyOutPutDir, sumPvUvOutPutDir , jobOptionModel.getFrequency(), "pv,uv")){
				logger.error("RunAnalyInventoryFrequencyJob SumInventoryFrequencyPVUV imp proccess is faild");
				return false;
			}
		}
		
		//分析点击日志
		if(clkStatus){
			paths = CommonUtil.clkLog(strDate);//读取点击日志
		}
		if(paths==null||paths.length==0){
			logger.warn("have no log for clk in " + strDate);
			return false;
		}else{
			if(CommonUtil.judgeDir(analyOutPutDir)){
				CommonUtil.delete(analyOutPutDir);
			}
			if(!jobControl.AnalyInventoryFrequencyPVUV(analyOutPutDir, paths, jobOptionModel.getOrder(), 
					jobOptionModel.getSite(), jobOptionModel.getInventory(), jobOptionModel.getCollect())){
				logger.error("RunAnalyInventoryFrequencyJob AnalyInventoryFrequencyPVUV clk proccess is faild");
				return false;
			}
			
			if(CommonUtil.judgeDir(sumClkUcOutPutDir)){
				CommonUtil.delete(sumClkUcOutPutDir);
			}
			//分析结果本地存放位置
			if(!jobControl.SumInventoryFrequencyPVUV(analyOutPutDir, sumClkUcOutPutDir , jobOptionModel.getFrequency(), "clk,uc")){
				logger.error("RunAnalyInventoryFrequencyJob SumInventoryFrequencyPVUV clk proccess is faild");
				return false;
			}
		}
		
		//合并分析后的展现（PV、UV）数据和点击（CLK、UV）数据
		paths = CommonUtil.impAndClkLog(sumPvUvOutPutDir, sumClkUcOutPutDir);
		if(paths==null||paths.length==0){
			logger.warn("sumImpOutputDir : " + sumPvUvOutPutDir + 
					"\n sumClkOutputDir : " + sumClkUcOutPutDir + " is not exist");
			return false;
		}else{
			if(CommonUtil.judgeDir(sumPvUvClkUcMergeOutPutDir)){
				CommonUtil.delete(sumPvUvClkUcMergeOutPutDir);
			}
			
			if(!jobControl.CombineInventoryFrequencyPvUvClkUcJob(paths, sumPvUvClkUcMergeOutPutDir ,resultnorm)){
				logger.error("RunAnalyInventoryFrequencyJob CombineInventoryFrequencyPvUvClkUcJob proccess is faild");
				return false;
			}
		}
		
		//根据频次前面的数据分组数据，且根据频次排序数据  及实现group by 和 order by asc
		paths = CommonUtil.listInPutDir(sumPvUvClkUcMergeOutPutDir);
		if(paths==null||paths.length==0){
			logger.warn("sumPvUvClkUcMergeOutPutDir : " + sumPvUvClkUcMergeOutPutDir + " is not exist");
			return false;
		}else{
			if(CommonUtil.judgeDir(sortPvUvClkUcMergeOutPutDir)){
				CommonUtil.delete(sortPvUvClkUcMergeOutPutDir);
			}
			//默认  非汇总
			int frequencyIndex = 3;
			int inputLineLength = 8; 
			if("1".equals(jobOptionModel.getCollect())){//汇总
				frequencyIndex = 2;//频次字段所在列的位置 从0开始计数
				inputLineLength = 7;//每列字段的个数
			}
			if(!jobControl.SortFrequencyPvUvClkUcJob(paths, sortPvUvClkUcMergeOutPutDir , frequencyIndex, inputLineLength)){
				logger.error("RunAnalyInventoryFrequencyJob SortFrequencyPvUvClkUcJob proccess is faild");
				return false;
			}
			//String localDir = "InventoryFrequency";
			//CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, sortPvUvClkUcMergeOutPutDir, localDir, "customize");
		}
		return isSuccessful;
	}
		
		
/*	public static void main(String args[])throws Exception{
		String beginDate = null;
		String endDate  = null ; 
		String queue = "queue1";
		String frequency = null;
		String orderId = null;
		String mediaPar = null;//广告位
		String inventoryidPar = null;//广告位
		String collectPar = null;//是否汇总 1是  0否
		if(args.length<8||args==null){
		    System.out.println("[-B <beginTime>]" +
                    "[-E <endTime>] " +
                    "[-Q <queue>] "+
                    "[-IOC<imp or clk>(pv uv clk uc)]"+
                    "[-Fre < frequency >]"+
                    "[-OD<orderid>]"+
                    "[-SiteIDs < siteid >]"+
                    "[-Inv < inventoryid >]"+
                    "[-Col < colect >]");
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
				}else if("-Fre".equals(args[i])){
					frequency = args[++i];
				}else if("-SiteIDs".equals(args[i])){//媒体
					mediaPar = args[++i];
				}else if("-Inv".equals(args[i])){//广告位
					inventoryidPar = args[++i];
				}else if("-Col".equals(args[i])){//是否是汇总
					collectPar = args[++i];
				}
			}
		}//hadoop jar UT.jar RunAnalyInventoryFrequencyPvUvClkUcMergeJob -B 2013-08-11 -E 2013-08-11 -Q queue3 -OD 1733 -Fre 10 -SiteIDs 1 -Inv 3612,2231,2230,2073 -Col 0
		boolean impStatus = true;
		boolean clkStatus = true;
		String strDate = CommonUtil.strStartAndEndTime(beginDate, endDate);//返回日志时间
		String analyOutPutDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/AnalyInventoryFrequencyPvUvClkUc/output";
		String sumPvUvOutPutDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/SumInventoryFrequencyPvUv/output";
		String sumClkUcOutPutDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/SumInventoryFrequencyClkUc/output";
		String sumPvUvClkUcMergeOutPutDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/SumInventoryFrequencyPvUvClkUcMerge/output";
		String sortPvUvClkUcMergeOutPutDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/SortInventoryFrequencyPvUvClkUcMerge/output";
		Path [] paths = null ;
		
		//分析展现日志
		if(impStatus){//读取展现日志
			paths = CommonUtil.impLog(strDate);
		}
		if(paths==null||paths.length==0){
			System.out.println("have no log");
			System.exit(0);
		}else{
			if(CommonUtil.judgeDir(analyOutPutDir)){
				CommonUtil.delete(analyOutPutDir);
			}
			JobControl.AnalyInventoryFrequencyPVUV(analyOutPutDir, queue, paths, orderId, mediaPar, inventoryidPar, collectPar);
			if(CommonUtil.judgeDir(sumPvUvOutPutDir)){
				CommonUtil.delete(sumPvUvOutPutDir);
			}
			//分析结果本地存放位置
			JobControl.SumInventoryFrequencyPVUV(analyOutPutDir, queue, sumPvUvOutPutDir , frequency, "pv,uv");
		}
		
		//分析点击日志
		if(clkStatus){
			paths = CommonUtil.clkLog(strDate);//读取点击日志
		}
		if(paths==null||paths.length==0){
			System.out.println("have no log");
			System.exit(0);
		}else{
			if(CommonUtil.judgeDir(analyOutPutDir)){
				CommonUtil.delete(analyOutPutDir);
			}
			JobControl.AnalyInventoryFrequencyPVUV(analyOutPutDir, queue, paths, orderId, mediaPar, inventoryidPar, collectPar);
			if(CommonUtil.judgeDir(sumClkUcOutPutDir)){
				CommonUtil.delete(sumClkUcOutPutDir);
			}
			//分析结果本地存放位置
			JobControl.SumInventoryFrequencyPVUV(analyOutPutDir, queue, sumClkUcOutPutDir , frequency, "clk,uc");
		}
		
		//合并分析后的展现（PV、UV）数据和点击（CLK、UV）数据
		paths = CommonUtil.impAndClkLog(sumPvUvOutPutDir, sumClkUcOutPutDir);
		if(paths==null||paths.length==0){
			System.out.println("have no log");
			System.exit(0);
		}else{
			if(CommonUtil.judgeDir(sumPvUvClkUcMergeOutPutDir)){
				CommonUtil.delete(sumPvUvClkUcMergeOutPutDir);
			}
			JobControl.CombineInventoryFrequencyPvUvClkUcJob(paths, queue, sumPvUvClkUcMergeOutPutDir ,"pv,uv,clk,uc");
			//分析结果本地存放位置
//			String localDir = "InventoryFrequencyPvUvClkUcMerge";
			String localDir = "InventoryFrequency";
			CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, sumPvUvClkUcMergeOutPutDir, localDir, "customize");
		}
		
		//根据频次前面的数据分组数据，且根据频次排序数据  及实现group by 和 order by asc
		paths = CommonUtil.listInPutDir(sumPvUvClkUcMergeOutPutDir);
//		paths = CommonUtil.listInPutDir(sumPvUvClkUcMergeOutPutDir);
		if(paths==null||paths.length==0){
			System.out.println("have no log");
			System.exit(0);
		}else{
			if(CommonUtil.judgeDir(sortPvUvClkUcMergeOutPutDir)){
				CommonUtil.delete(sortPvUvClkUcMergeOutPutDir);
			}
			//默认  非汇总
			int frequencyIndex = 3;
			int inputLineLength = 8; 
			if("1".equals(collectPar)){//汇总
				frequencyIndex = 2;//频次字段所在列的位置 从0开始计数
				inputLineLength = 7;//每列字段的个数
			}
			JobControl.SortFrequencyPvUvClkUcJob(paths, queue, sortPvUvClkUcMergeOutPutDir , frequencyIndex, inputLineLength);
			//分析结果本地存放位置
//			String localDir = "InventoryFrequencyPvUvClkUcMerge";
			String localDir = "InventoryFrequency";
			CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, sortPvUvClkUcMergeOutPutDir, localDir, "customize");
		}
	}
*/
}
