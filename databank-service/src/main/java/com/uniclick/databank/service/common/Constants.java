/**
 * @Title: Constants.java
 * @Package com.uniclick.databank.common
 * @Description: TODO(用一句话描述该文件做什么)
 * @author ping.jie
 * @date 2015-4-10 上午11:19:42
 * @version V1.0
 */

package com.uniclick.databank.service.common;

/**
 * @ClassName: Constants
 * @Description: TODO(常量)
 * @author ping.jie
 * @date 2015-4-10 上午11:19:42
 * @verision $Id
 * 
 */

public class Constants {

	/*
	 * 对应数据库中描述JOB的几种状态
	 * 0 ：待执行
	 * 1：成功
	 * 2：开始任务
	 * 3：生成excel成功
	 * 4：失败
	 */
	public static final int JOB_STATUS_EXECUTORY = 0;
	public static final int JOB_STATUS_SUCCESS = 1;
	public static final int JOB_STATUS_STARTJOB = 2;
	public static final int JOB_STATUS_BUILDEREXCEL = 3;
	public static final int JOB_STATUS_FAILED = 4;
	
	/*
	 * 自定义所使用的队列
	 */
	public static final String QUEUE = "queue3";
	/*
	 * 结果指标英文名与中文名称对应关系 配置文件
	 */
	public static final String RESULTNORM_PROP = "resultNorm.properties";
}
