package com.uniclick.databank.service.ut.pojo;

import java.util.Date;
/**
 * 
 * @ClassName: TblCustomReportJob
 * @Description: TODO(tbl_custom_report_job 表对象)
 * @author ping.jie
 * @date 2015-4-9 下午3:42:39
 * @verision $Id
 *
 */
public class TblCustomReportJob {

	private	Integer	 id 	;
	private	Integer	jobType	;
	private String jobName;
	private	String 	 idCollection	;//JOB执行条件
	private	Integer	breakDown 	;
	private	String 	metrics 	;//结果指标
	private	String 	 startDate 	;
	private	String 	endDate	;
	private	String 	mailTo	;
	private	String 	mailCc	;
	private	String 	mailTitle 	;
	private	String 	mailContent 	;
	private	Integer	status	;//状态
	private String filePath ;
	private	Date 	createdBy 	;
	private	Date 	createdAt	;
	private String excelPath;
	
	public Integer getId() {
		return id;
	}
	public void setId(Integer id) {
		this.id = id;
	}
	
	/**
	 * @return jobName
	 */
	
	public String getJobName() {
		return jobName;
	}
	/**
	 * @param jobName 要设置的 jobName
	 */
	
	public void setJobName(String jobName) {
		this.jobName = jobName;
	}
	public Integer getJobType() {
		return jobType;
	}
	public void setJobType(Integer jobType) {
		this.jobType = jobType;
	}
	public String getIdCollection() {
		return idCollection;
	}
	public void setIdCollection(String idCollection) {
		this.idCollection = idCollection;
	}
	public Integer getBreakDown() {
		return breakDown;
	}
	public void setBreakDown(Integer breakDown) {
		this.breakDown = breakDown;
	}
	public String getMetrics() {
		return metrics;
	}
	public void setMetrics(String metrics) {
		this.metrics = metrics;
	}
	
	/**
	 * @return startDate
	 */
	
	public String getStartDate() {
		return startDate;
	}
	/**
	 * @param startDate 要设置的 startDate
	 */
	
	public void setStartDate(String startDate) {
		this.startDate = startDate;
	}
	/**
	 * @return endDate
	 */
	
	public String getEndDate() {
		return endDate;
	}
	/**
	 * @param endDate 要设置的 endDate
	 */
	
	public void setEndDate(String endDate) {
		this.endDate = endDate;
	}
	public String getMailTo() {
		return mailTo;
	}
	public void setMailTo(String mailTo) {
		this.mailTo = mailTo;
	}
	public String getMailCc() {
		return mailCc;
	}
	public void setMailCc(String mailCc) {
		this.mailCc = mailCc;
	}
	public String getMailTitle() {
		return mailTitle;
	}
	public void setMailTitle(String mailTitle) {
		this.mailTitle = mailTitle;
	}
	public String getMailContent() {
		return mailContent;
	}
	public void setMailContent(String mailContent) {
		this.mailContent = mailContent;
	}
	public Integer getStatus() {
		return status;
	}
	public void setStatus(Integer status) {
		this.status = status;
	}
	public Date getCreatedBy() {
		return createdBy;
	}
	public void setCreatedBy(Date createdBy) {
		this.createdBy = createdBy;
	}
	public Date getCreatedAt() {
		return createdAt;
	}
	public void setCreatedAt(Date createdAt) {
		this.createdAt = createdAt;
	}
	public String getFilePath() {
		return filePath;
	}
	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}
	/**
	 * @return excelPath
	 */
	
	public String getExcelPath() {
		return excelPath;
	}
	/**
	 * @param excelPath 要设置的 excelPath
	 */
	
	public void setExcelPath(String excelPath) {
		this.excelPath = excelPath;
	}

	
}
