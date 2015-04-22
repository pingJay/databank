/**
 * @Title: CustomJobFactory.java
 * @Package com.uniclick.databank.mapreduce.util
 * @Description: TODO(用一句话描述该文件做什么)
 * @author ping.jie
 * @date 2015-4-14 下午4:05:11
 * @version V1.0
 */

package com.uniclick.databank.mapreduce.util;

import com.uniclick.databank.mapreduce.job.RunActCrowdJob;
import com.uniclick.databank.mapreduce.job.RunActJob;
import com.uniclick.databank.mapreduce.job.RunActRegionJob;
import com.uniclick.databank.mapreduce.job.RunAnalyInventoryFrequencyJob;
import com.uniclick.databank.mapreduce.job.RunAnalyOrderFrequencyJob;
import com.uniclick.databank.mapreduce.job.RunAnalyOrderSiteFrequencyJob;
import com.uniclick.databank.mapreduce.job.RunBrowsingHabitsJob;
import com.uniclick.databank.mapreduce.job.RunCompanyJob;
import com.uniclick.databank.mapreduce.job.RunInventoryJob;
import com.uniclick.databank.mapreduce.job.RunMediaAreaJob;
import com.uniclick.databank.mapreduce.job.RunMediaHourJob;
import com.uniclick.databank.mapreduce.job.RunMediaJob;
import com.uniclick.databank.mapreduce.job.RunOrderAreaJob;
import com.uniclick.databank.mapreduce.job.RunOrderCrowdJob;
import com.uniclick.databank.mapreduce.job.RunOrderItemJob;
import com.uniclick.databank.mapreduce.job.RunOrderJob;
import com.uniclick.databank.mapreduce.job.RunOrderOrderItemRefreralJob;
import com.uniclick.databank.mapreduce.job.RunOrderSaturationJob;
import com.uniclick.databank.mapreduce.job.RunOrderSiteCoincideJob;
import com.uniclick.databank.mapreduce.job.RunSiteCrowdJob;
import com.uniclick.databank.mapreduce.job.RunSiteMobileDevicesJob;
import com.uniclick.databank.mapreduce.job.RunSiteUserDevicesTypeJob;
import com.uniclick.databank.mapreduce.job.RunUniFrequencyJob;
import com.uniclick.databank.mapreduce.job.iface.RunCustomJob;

/**
 * @ClassName: CustomJobFactory
 * @Description: TODO(自定义JOB 工厂方法)
 * @author ping.jie
 * @date 2015-4-14 下午4:05:11
 * @verision $Id
 * 
 */

public class CustomJobFactory {
	public static final String RUNCOMPANYJOB = "RunCompanyJob";
	public static final String RUNORDERJOB = "RunOrderJob";
	public static final String RUNINVENTORY = "RunInventoryJob";
	public static final String RUNORDERITEMJOB = "RunOrderItemJob";
	public static final String RUNMEDIAJOB = "RunMediaJob";
	public static final String RUNORDERAREAJOB = "RunOrderAreaJob";
	public static final String RUNMEDIAAREAJOB = "RunMediaAreaJob";
	public static final String RUNORDERSITECOINCIDEJOB = "RunOrderSiteCoincideJob";
	public static final String RUNANALYORDERFREQUENCYJOB = "RunAnalyOrderFrequencyJob";
	public static final String RUNANALYORDERSITEFREQUENCYJOB = "RunAnalyOrderSiteFrequencyJob";
	public static final String RUNORDERCROWDJOB = "RunOrderCrowdJob";
	public static final String RUNACTCROWDJOB = "RunActCrowdJob";
	public static final String RUNACTREGIONJOB = "RunActRegionJob";
	public static final String RUNACTJOB = "RunActJob";
	public static final String RUNORDERORDERITEMFRERALJOB = "RunOrderOrderItemRefreralJob";
	public static final String RUNORDERSATURATIONJOB = "RunOrderSaturationJob";
	public static final String RUNMEDIAHOURJOB = "RunMediaHourJob";
	public static final String RUNBROWSINGHABITSJOB = "RunBrowsingHabitsJob";
	public static final String RUNANALYINVENTORYFREQUENCYJOB = "RunAnalyInventoryFrequencyJob";
	public static final String RUNSITECROWDJOB = "RunSiteCrowdJob";
	public static final String RUNUNIFREQUENCYJOB = "RunUniFrequencyJob";
	public static final String RUNSITEMOBILEDEVICESJOB = "RunSiteMobileDevicesJob";
	public static final String RUNSITEUSERDEVICESTYPEJOB = "RunSiteUserDevicesTypeJob";
	
	public static RunCustomJob getCustomJob(String jobClassName,String queue) {
		if(RUNCOMPANYJOB.equals(jobClassName)) {
			return new RunCompanyJob(queue);
		}else if(RUNORDERJOB.equals(jobClassName)) {
			return new RunOrderJob(queue);
		}else if(RUNINVENTORY.equals(jobClassName)) {
			return new RunInventoryJob(queue);
		}else if(RUNORDERITEMJOB.equals(jobClassName)) {
			return new RunOrderItemJob(queue);
		}else if(RUNMEDIAJOB.equals(jobClassName)) {
			return new RunMediaJob(queue);
		}else if(RUNORDERAREAJOB.equals(jobClassName)) {
			return new RunOrderAreaJob(queue);
		}else if(RUNMEDIAAREAJOB.equals(jobClassName)) {
			return new RunMediaAreaJob(queue);
		}else if(RUNORDERSITECOINCIDEJOB.equals(jobClassName)) {
			return new RunOrderSiteCoincideJob(queue);
		}else if(RUNANALYORDERFREQUENCYJOB.equals(jobClassName)) {
			return new RunAnalyOrderFrequencyJob(queue);
		}else if(RUNANALYORDERSITEFREQUENCYJOB.equals(jobClassName)) {
			return new RunAnalyOrderSiteFrequencyJob(queue);
		}else if(RUNORDERCROWDJOB.equals(jobClassName)) {
			return new RunOrderCrowdJob(queue);
		}else if(RUNACTCROWDJOB.equals(jobClassName)) {
			return new RunActCrowdJob(queue);
		}else if(RUNACTREGIONJOB.equals(jobClassName)) {
			return new RunActRegionJob(queue);
		}else if(RUNACTJOB.equals(jobClassName)) {
			return new RunActJob(queue);
		}else if(RUNORDERORDERITEMFRERALJOB.equals(jobClassName)) {
			return new RunOrderOrderItemRefreralJob(queue);
		}else if(RUNORDERSATURATIONJOB.equals(jobClassName)) {
			return new RunOrderSaturationJob(queue);
		}else if(RUNMEDIAHOURJOB.equals(jobClassName)) {
			return new RunMediaHourJob(queue);
		}else if(RUNBROWSINGHABITSJOB.equals(jobClassName)) {
			return new RunBrowsingHabitsJob(queue);
		}else if(RUNANALYINVENTORYFREQUENCYJOB.equals(jobClassName)) {
			return new RunAnalyInventoryFrequencyJob(queue);
		}else if(RUNSITECROWDJOB.equals(jobClassName)) {
			return new RunSiteCrowdJob(queue);
		}else if(RUNUNIFREQUENCYJOB.equals(jobClassName)) {
			return new RunUniFrequencyJob(queue);
		}else if(RUNSITEMOBILEDEVICESJOB.equals(jobClassName)) {
			return new RunSiteMobileDevicesJob(queue);
		}else if(RUNSITEUSERDEVICESTYPEJOB.equals(jobClassName)) {
			return new RunSiteUserDevicesTypeJob(queue);
		}
		return null;
	}
	
}
