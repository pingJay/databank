/**
 * @Title: CustomJobRunService.java
 * @Package com.uniclick.databank.ut.service
 * @Description: TODO(用一句话描述该文件做什么)
 * @author ping.jie
 * @date 2015-4-10 下午1:31:58
 * @version V1.0
 */

package com.uniclick.databank.service.ut.service;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uniclick.databank.email.MailConstants;
import com.uniclick.databank.email.SendMailToUser;
import com.uniclick.databank.mapreduce.job.iface.RunCustomJob;
import com.uniclick.databank.mapreduce.util.CustomJobFactory;
import com.uniclick.databank.service.common.Constants;
import com.uniclick.databank.service.common.DbBean;
import com.uniclick.databank.service.ut.SepcialJobInfo;
import com.uniclick.databank.service.ut.persist.UTCustomJobTable;
import com.uniclick.databank.service.ut.persist.UTCustomReportTable;
import com.uniclick.databank.service.ut.pojo.TblCustomJobBasic;
import com.uniclick.databank.service.ut.pojo.TblCustomReportJob;
import com.uniclick.excelbuilder.ExcelBuilder;


/**
 * @ClassName: CustomJobRunService
 * @Description: TODO(执行自定义任务的服务类)
 * @author ping.jie
 * @date 2015-4-10 下午1:31:58
 * @verision $Id
 * 
 */

public class CustomJobRunService {
	private static final Logger logger = LoggerFactory.getLogger(CustomJobRunService.class);
	private static final int MAXJOB = 2;//最大同时运行JOB数
	private SendMailToUser sendMail = new SendMailToUser();
	private UTCustomReportTable utCustomReportTable;
	private UTCustomJobTable utCustomJobTable;
	private DbBean dbBean;
	public void init() {
		dbBean = new DbBean();
		utCustomReportTable = new UTCustomReportTable(dbBean);
		utCustomJobTable = new UTCustomJobTable(dbBean);
		logger.debug("init Success");
	}
	
	public void run(){
		this.init();
		/*
		 * 清理库中执行超过一天任然没有完成的任务，将这个任务状态直接改为失败
		 */
		if(!utCustomReportTable.cleanProblemJob()) {
			logger.error("clean history Job faild");
		}
		int runningJobNum = utCustomReportTable.getRunningJobNum();
		if(runningJobNum >= MAXJOB) {
			logger.info("running Job number exceed MAX value");
		}else{
			List<TblCustomReportJob> exectoryJobList = utCustomReportTable.getExectoryJobs();
			while(runningJobNum < MAXJOB && exectoryJobList.size() > 0) {
				TblCustomReportJob reportJob = exectoryJobList.get(0);
				logger.info("JOB " + reportJob.getId() + " MR start running");
				this.executeAssignJob(reportJob);
				runningJobNum = utCustomReportTable.getRunningJobNum();
				exectoryJobList = utCustomReportTable.getExectoryJobs();
			}
		}
		this.cleanUp();
	}
	
	public void cleanUp(){
		this.dbBean.close();
	}
	
	public void executeAssignJob(TblCustomReportJob reportJob) {
		TblCustomJobBasic jobBasic = utCustomJobTable.getTblCustomJobBasic(reportJob.getJobType());
		if(jobBasic != null) {
			/*
			 * 通过工厂方法实例化要执行的job对象
			 */
			RunCustomJob runCustomJob = CustomJobFactory.getCustomJob(jobBasic.getJobClassName(), Constants.QUEUE);
			utCustomReportTable.updateJobStatus(reportJob.getId(), Constants.JOB_STATUS_STARTJOB);
			boolean runMRCompile = runCustomJob.run(reportJob.getStartDate(), reportJob.getEndDate(), reportJob.getIdCollection(), reportJob.getMetrics());
			/*
			 * Mapreduce 分析程序执行完成
			 */
			if(runMRCompile) {
				logger.debug("CUSTOMJOB ID " + reportJob.getId() + " Mapreduce complete");
				/*
				 * 准备生成excel 所需参数
				 */
				String resultPath = String.format(jobBasic.getHdfsResultDir(), reportJob.getStartDate(),reportJob.getEndDate());
				logger.info("resultPath : " + resultPath);
				String businessTime = String.format("%s_%s",  reportJob.getStartDate(),reportJob.getEndDate());
				logger.info("businessTime : " + businessTime);
				String resultNorm = SepcialJobInfo.getFullResultNorm(jobBasic.getJobClassName(), reportJob.getIdCollection(),
						jobBasic.getResultNorm(), reportJob.getMetrics());
				logger.info("resultNorm : " + resultNorm);
				String reportName = jobBasic.getJobName();
				logger.info("reportName : " + reportName);
				String excelPath = String.format(jobBasic.getExcelDir(), reportJob.getJobName());
				logger.info("excelPath : " + excelPath);
				ExcelBuilder excelBuilder = new ExcelBuilder(resultPath, excelPath, resultNorm.toString(), 
						reportName, businessTime, jobBasic.isConverId());
				/*
				 * excel 生成成功
				 */
				boolean buildExcelCompile = excelBuilder.builder();
				if(buildExcelCompile) {
					reportJob.setExcelPath(excelPath);
					utCustomReportTable.updateExcelPath(reportJob);
					utCustomReportTable.updateJobStatus(reportJob.getId(), Constants.JOB_STATUS_BUILDEREXCEL);
					logger.debug("CUSTOMJOB ID " + reportJob.getId() + " excelBuilder complete. \n" +
							"excelPath : " + excelPath);
					/*
					 * 准备发邮件参数
					 */
					String[] to = reportJob.getMailTo().split(",");
					String[] mailCC = (("".equals(reportJob.getMailCc())||reportJob.getMailCc()==null)?null:(reportJob.getMailCc().split(",")));
					String mailTile = reportJob.getMailTitle();
					String mailContext = reportJob.getMailContent();
					String jobName = reportJob.getJobName() + ".xls";
					sendMail.execMailToUser(MailConstants.MAIL_FROM, to,mailCC,null, mailTile,mailContext,excelPath,jobName);
					utCustomReportTable.updateJobStatus(reportJob.getId(), Constants.JOB_STATUS_SUCCESS);
				}else{
					logger.error("JOBID : " + reportJob.getId() + " excel builder process faild");
					utCustomReportTable.updateJobStatus(reportJob.getId(), Constants.JOB_STATUS_FAILED);
					System.exit(1);
				}
			}else{
				logger.error("JOBID : " + reportJob.getId() + " MR exce process  faild");
				utCustomReportTable.updateJobStatus(reportJob.getId(), Constants.JOB_STATUS_FAILED);
				System.exit(1);
			}
		}
	}
	
	public static void main(String[] args) {
		CustomJobRunService jobRunService = new CustomJobRunService();
		jobRunService.run();
	}
}
