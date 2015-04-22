/**
 * @Title: SepcialJobInfo.java
 * @Package com.uniclick.databank.service.ut
 * @Description: TODO(用一句话描述该文件做什么)
 * @author ping.jie
 * @date 2015-4-15 下午5:53:52
 * @version V1.0
 */

package com.uniclick.databank.service.ut;

import com.uniclick.databank.mapreduce.pojo.MapReduceJobOptionModel;
import com.uniclick.databank.mapreduce.util.CustomJobFactory;
import com.uniclick.databank.mapreduce.util.JobOptionsParse;
import com.uniclick.excelbuilder.common.Constants;

/**
 * @ClassName: SepcialJobInfo
 * @Description: TODO(针对一些特殊的JOB的一些特殊信息处理)
 * @author ping.jie
 * @date 2015-4-15 下午5:53:52
 * @verision $Id
 * 
 */

public class SepcialJobInfo {
	
	/**
	 * 
	 * @Title: getFullResultNorm
	 * @Description: TODO(获取一个具体JOB的完整结果列)
	 * @param @param fixResultRow 这种类型JOB的固定结果列
	 * @param @param optionalResultRow  可选择的结果列
	 * @param @return    设定文件
	 * @return String    返回类型
	 * @throws
	 */
	public static String getFullResultNorm(String jobClassName,String idCollect,String fixResultRow,String optionalResultRow) {
		StringBuilder resultNorm = new StringBuilder();
		resultNorm.append(fixResultRow);
		MapReduceJobOptionModel jobOptinModer = JobOptionsParse.parse(idCollect);
		/**
		 * 媒体重合度定制报表
		 */
		if(CustomJobFactory.RUNORDERSITECOINCIDEJOB.equals(jobClassName)) {
			int coincide = Integer.parseInt(jobOptinModer.getCoincide());
			if(coincide == 3) {
				resultNorm.append(",")
				.append(Constants.MEDIA);
			}else if(coincide == 4){
				resultNorm.append(",").append(Constants.MEDIA)
					.append(",").append(Constants.MEDIA);
			}else if(coincide == 4){
				resultNorm.append(",").append(Constants.MEDIA)
				.append(",").append(Constants.MEDIA)
				.append(",").append(Constants.MEDIA);
			}
		}else if(CustomJobFactory.RUNANALYINVENTORYFREQUENCYJOB.equals(jobClassName)) {
			int collect = Integer.parseInt(jobOptinModer.getCollect());
			if(collect == 0) {
				resultNorm.append(",").append(Constants.MEDIA)
				.append(",").append(Constants.INVERTORY)
				.append(",").append(Constants.FREQUENCY);
			}else if(collect == 1) {
				resultNorm.append(",").append(Constants.INVERTORY)
				.append(",").append(Constants.FREQUENCY);
			}
		}
		
		if(!"".equals(optionalResultRow)){
			resultNorm.append(",").append(optionalResultRow);
		}
		
		return resultNorm.toString();
	}
}
