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
 * @ClassName: RunAnalyOrderFrequencyJob
 * @Description: TODO(分析指定活动频次PV UV CLK UC数据  )
 * @author ping.jie
 * @date 2015-4-14 上午9:41:00
 * @verision $Id
 *
 */
public class RunAnalyOrderFrequencyJob implements RunCustomJob{

	private static final Logger logger = LoggerFactory.getLogger(RunAnalyOrderFrequencyJob.class);
	private JobControl jobControl;
	
	public RunAnalyOrderFrequencyJob(String queue) {
		jobControl = new JobControl(queue);
	}
	
	public boolean run(String beginDate,String endDate,String jobOptins,String resultnorm) {
		boolean isSuccessful = true;
		boolean impStatus = true;
		boolean clkStatus = true;
		MapReduceJobOptionModel mrOptionModel = JobOptionsParse.parse(jobOptins);
		String strDate = CommonUtil.strStartAndEndTime(beginDate, endDate);//返回日志时间
		String analyOutPutDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/AnalyOrderFrequency/output";
		String sumPvUvOutPutDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/SumOrderFrequencyPvUv/output";
		String sumClkUcOutPutDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/SumOrderFrequencyClkUc/output";
		String sumPvUvClkUcMergeOutPutDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/SumOrderFrequencyPvUvClkUcMerge/output";
		String sortPvUvClkUcMergeOutPutDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/SortOrderFrequencyPvUvClkUcMerge/output";
		Path [] paths = null ;
		
		//展现日志
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
			
			if(!jobControl.AnalyOrderFrequencyPVUV(analyOutPutDir, paths, mrOptionModel.getOrder())){
				logger.error("RunAnalyOrderFrequencyJob AnalyOrderFrequencyPVUV imp proccess is faild");
				return false;
			}
			if(CommonUtil.judgeDir(sumPvUvOutPutDir)){
				CommonUtil.delete(sumPvUvOutPutDir);
			}
			//分析结果本地存放位置
			if(!jobControl.SumOrderFrequencyPVUV(analyOutPutDir, sumPvUvOutPutDir , mrOptionModel.getFrequency(), "pv,uv")){
				logger.error("RunAnalyOrderFrequencyJob SumOrderFrequencyPVUV imp proccess is faild");
				return false;
			}
		}
		
		
		//点击日志
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
			if(!jobControl.AnalyOrderFrequencyPVUV(analyOutPutDir, paths, mrOptionModel.getOrder())){
				logger.error("RunAnalyOrderFrequencyJob AnalyOrderFrequencyPVUV clk proccess is faild");
				return false;
			}
			if(CommonUtil.judgeDir(sumClkUcOutPutDir)){
				CommonUtil.delete(sumClkUcOutPutDir);
			}
			//分析结果本地存放位置
			if(!jobControl.SumOrderFrequencyPVUV(analyOutPutDir, sumClkUcOutPutDir , mrOptionModel.getFrequency(), "clk,uc")){
				logger.error("RunAnalyOrderFrequencyJob SumOrderFrequencyPVUV clk proccess is faild");
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
			
			if(!jobControl.CombineOrderFrequencyPvUvClkUcJob(paths, sumPvUvClkUcMergeOutPutDir ,resultnorm)){
				logger.error("RunAnalyOrderFrequencyJob CombineOrderFrequencyPvUvClkUcJob proccess is faild");
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
			int frequencyIndex = 1;
			int inputLineLength = 6; 
			
			if(!jobControl.SortFrequencyPvUvClkUcJob(paths, sortPvUvClkUcMergeOutPutDir , frequencyIndex, inputLineLength)){
				logger.error("RunAnalyOrderFrequencyJob SortFrequencyPvUvClkUcJob proccess is faild");
				return false;
			}
			//分析结果本地存放位置
			//String localDir = "OrderFrequency";
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
//		String impOrClk  = null;//区别分析点击日志或是展现日志
		if(args.length<8||args==null){
		    System.out.println("[-B <beginTime>]" +
                    "[-E <endTime>] " +
                    "[-Q <queue>] "+
                    "[-IOC<imp or clk>]"+
                    "[-Fre < frequency >]"+
                    "[-OD<orderid>]");
		    System.exit(0);
		}else{
			for(int i=0 ;i<args.length ; i++){
				if("-B".equals(args[i])){
					beginDate = args[++i];
				}else if("-E".equals(args[i])){
					endDate = args[++i];
				}else if("-Q".equals(args[i])){
					queue = args[++i];
				}
//				else if("-IOC".equals(args[i])){
//					impOrClk = args[++i];
//				}
				else if("-OD".equals(args[i])){
					orderId = args[++i];
				}else if("-Fre".equals(args[i])){
					frequency = args[++i];
				}
			}
		}//hadoop jar UT.jar RunAnalyOrderFrequencyPvUvClkUcMergeJob -B 2013-08-11 -E 2013-08-11 -Q queue3 -OD 1733 -Fre 10
		boolean impStatus = true;
		boolean clkStatus = true;
		String strDate = CommonUtil.strStartAndEndTime(beginDate, endDate);//返回日志时间
		String analyOutPutDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/AnalyOrderFrequency/output";
		String sumPvUvOutPutDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/SumOrderFrequencyPvUv/output";
		String sumClkUcOutPutDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/SumOrderFrequencyClkUc/output";
		String sumPvUvClkUcMergeOutPutDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/SumOrderFrequencyPvUvClkUcMerge/output";
		String sortPvUvClkUcMergeOutPutDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/SortOrderFrequencyPvUvClkUcMerge/output";
		Path [] paths = null ;
		
		//展现日志
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
			JobControl.AnalyOrderFrequencyPVUV(analyOutPutDir, queue, paths, orderId);
			if(CommonUtil.judgeDir(sumPvUvOutPutDir)){
				CommonUtil.delete(sumPvUvOutPutDir);
			}
			//分析结果本地存放位置
			JobControl.SumOrderFrequencyPVUV(analyOutPutDir, queue, sumPvUvOutPutDir , frequency, "pv,uv");
		}
		
		
		//点击日志
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
			JobControl.AnalyOrderFrequencyPVUV(analyOutPutDir, queue, paths, orderId);
			if(CommonUtil.judgeDir(sumClkUcOutPutDir)){
				CommonUtil.delete(sumClkUcOutPutDir);
			}
			//分析结果本地存放位置
			JobControl.SumOrderFrequencyPVUV(analyOutPutDir, queue, sumClkUcOutPutDir , frequency, "clk,uc");
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
			JobControl.CombineOrderFrequencyPvUvClkUcJob(paths, queue, sumPvUvClkUcMergeOutPutDir ,"pv,uv,clk,uc");
			//分析结果本地存放位置
			String localDir = "OrderFrequency";
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
			int frequencyIndex = 1;
			int inputLineLength = 6; 
			JobControl.SortFrequencyPvUvClkUcJob(paths, queue, sortPvUvClkUcMergeOutPutDir , frequencyIndex, inputLineLength);
			//分析结果本地存放位置
			String localDir = "OrderFrequency";
			CommonUtil.ftpToLocal(beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate, sortPvUvClkUcMergeOutPutDir, localDir, "customize");
		}
		
	}
*/
}
