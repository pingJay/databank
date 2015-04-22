/**
 * @Title: RunCustomJob.java
 * @Package com.uniclick.databank.mapreduce.job
 * @Description: TODO(用一句话描述该文件做什么)
 * @author ping.jie
 * @date 2015-4-14 下午3:59:24
 * @version V1.0
 */

package com.uniclick.databank.mapreduce.job.iface;

/**
 * @ClassName: RunCustomJob
 * @Description: TODO(自定义报告Job执行类的接口)
 * @author ping.jie
 * @date 2015-4-14 下午3:59:24
 * @verision $Id
 * 
 */

public interface RunCustomJob {

	public boolean run(String beginDate,String endDate,String jobOptins,String resultnorm) ;
}
