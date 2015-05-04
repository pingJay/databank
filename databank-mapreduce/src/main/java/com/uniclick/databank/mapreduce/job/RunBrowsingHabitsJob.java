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
 * @ClassName: RunBrowsingHabitsJob
 * @Description: TODO(分析用户网站浏览习惯)
 * @author ping.jie
 * @date 2015-4-14 上午10:13:51
 * @verision 2.0.0
 *
 */
public class RunBrowsingHabitsJob implements RunCustomJob{
	private static final Logger logger = LoggerFactory.getLogger(RunBrowsingHabitsJob.class);
	private JobControl jobControl;
	
	public RunBrowsingHabitsJob(String queue) {
		jobControl = new JobControl(queue);
	}
	
	public boolean run(String beginDate,String endDate,String jobOptins,String resultnorm) {
		boolean isSuccessful = true;
		MapReduceJobOptionModel mrOptionModel = JobOptionsParse.parse(jobOptins);
		String strDate =	CommonUtil.strStartAndEndTime(beginDate,endDate);//返回日志时间
		String analyOutPutDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/AnalyBrowsingHabits/output";
		String sumOutPutDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/SumBrowsingHabits/output";
		String endOutPutDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/EndBrowsingHabits/output";
		Path [] paths =null ;
		paths = CommonUtil.impLog(strDate);
		if(paths==null||paths.length==0){
			logger.warn("have no log for imp in " + strDate);
			return false;
		}else{
			if(CommonUtil.judgeDir(analyOutPutDir)){
				CommonUtil.delete(analyOutPutDir);
			}
			//输出该活动下的所有cookie
			if(!jobControl.AnalyOrderCookie(analyOutPutDir, paths, mrOptionModel.getOrder())){
				logger.error("RunBrowsingHabitsJob AnalyOrderCookie proccess is faild");
				return false;
			}
			if(CommonUtil.judgeDir(sumOutPutDir)){
				CommonUtil.delete(sumOutPutDir);
			}
			strDate =	CommonUtil.strStartAndEndTime(beginDate, endDate);//返回日志时间
			paths = CommonUtil.impAndOutPutLog(strDate, analyOutPutDir);
			
			if(!jobControl.AnalyBrowsingHabits(sumOutPutDir, paths, mrOptionModel.getSite())){
				logger.error("RunBrowsingHabitsJob AnalyBrowsingHabits proccess is faild");
				return false;
			}
			
			if(CommonUtil.judgeDir(endOutPutDir)){
				CommonUtil.delete(endOutPutDir);
			}
			String localDir = "BrowsingHabits" ;
			if(!jobControl.SumOrderSiteCoincide(sumOutPutDir, endOutPutDir)){
				logger.error("RunBrowsingHabitsJob SumOrderSiteCoincide proccess is faild");
				return false;
			}
			//CommonUtil.ftpToLocal((beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate), endOutPutDir, localDir , "customize");
		}
		return isSuccessful;
	}
	
	
/*	public static void main(String args[])throws Exception{
		String orderBeginDate = null;
		String orderendDate  = null ; 
		String siteIds = null; 
		String beginDate = null;
		String endDate = null;
		String queue = "queue1"; 
		String orderId = null;
		if(args.length<8||args==null){
		    System.out.println("[-B <beginTime>]" +
                    "[-E <endTime>] " +
                    "[-OB <beginTime>]" +
                    "[-OE <endTime>] " +
                    "[-Q <queue>] "+
                    "[-OD <orderId>]"+
                    "[-SiteIDs <siteids>]" );
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
				}else if("-OB".equals(args[i])){
					orderBeginDate = args[++i];
				}else if("-OE".equals(args[i])){
					orderendDate = args[++i];
				}else if("-SiteIDs".equals(args[i])){
					siteIds = args[++i];
				}
			}
		}
		String strDate = null;
		strDate =	CommonUtil.strStartAndEndTime(orderBeginDate, orderendDate);//返回日志时间
		String analyOutPutDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/AnalyBrowsingHabits/output";
		String sumOutPutDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/SumBrowsingHabits/output";
		String endOutPutDir = "hdfs://namenode/user/deepsight/tmp/"+beginDate + "---" + endDate +"/EndBrowsingHabits/output";
		Path [] paths =null ;
		paths = CommonUtil.impLog(strDate);
		if(paths==null||paths.length==0){
			System.out.println("have no log");
			System.exit(0);
		}else{
			if(CommonUtil.judgeDir(analyOutPutDir)){
				CommonUtil.delete(analyOutPutDir);
			}
			//输出该活动下的所有cookie
			JobControl.AnalyOrderCookie(analyOutPutDir, queue, paths, orderId);
			if(CommonUtil.judgeDir(sumOutPutDir)){
				CommonUtil.delete(sumOutPutDir);
			}
			strDate =	CommonUtil.strStartAndEndTime(beginDate, endDate);//返回日志时间
			paths = CommonUtil.impAndOutPutLog(strDate, analyOutPutDir);
			JobControl.AnalyBrowsingHabits(sumOutPutDir, queue, paths, siteIds);
			if(CommonUtil.judgeDir(endOutPutDir)){
				CommonUtil.delete(endOutPutDir);
			}
			String localDir = "BrowsingHabits" ;
			JobControl.SumOrderSiteCoincide(sumOutPutDir, queue, endOutPutDir);
			CommonUtil.ftpToLocal((beginDate.equals(endDate)?beginDate:beginDate+"_"+endDate), endOutPutDir, localDir , "customize");
		}
	}
*/
}
