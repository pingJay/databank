/**
 * @Title: TblCustomJobBasic.java
 * @Package com.uniclick.databank.service.ut.pojo
 * @Description: TODO(用一句话描述该文件做什么)
 * @author ping.jie
 * @date 2015-4-14 下午7:44:34
 * @version V1.0
 */

package com.uniclick.databank.service.ut.pojo;

/**
 * @ClassName: TblCustomJobBasic
 * @Description: TODO(自定义job的基本属性对象)
 * @author ping.jie
 * @date 2015-4-14 下午7:44:34
 * @verision $Id
 * 
 */

public class TblCustomJobBasic {

	private int id;
	private String jobName;
	private String jobClassName;
	private String jobhsql;
	private String hdfsResultDir;
	private String excelDir;
	private String resultNorm;
	/*
	 * 是否需要把结果文件中的ID转换成中文名称
	 * 0：需要
	 * 1：不需要
	 */
	private int isConverId; 
	/**
	 * @return id
	 */
	
	public int getId() {
		return id;
	}
	/**
	 * @param id 要设置的 id
	 */
	
	public void setId(int id) {
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
	/**
	 * @return jobClassName
	 */
	
	public String getJobClassName() {
		return jobClassName;
	}
	/**
	 * @param jobClassName 要设置的 jobClassName
	 */
	
	public void setJobClassName(String jobClassName) {
		this.jobClassName = jobClassName;
	}
	/**
	 * @return jobhsql
	 */
	
	public String getJobhsql() {
		return jobhsql;
	}
	/**
	 * @param jobhsql 要设置的 jobhsql
	 */
	
	public void setJobhsql(String jobhsql) {
		this.jobhsql = jobhsql;
	}
	/**
	 * @return hdfsResultDir
	 */
	
	public String getHdfsResultDir() {
		return hdfsResultDir;
	}
	/**
	 * @param hdfsResultDir 要设置的 hdfsResultDir
	 */
	
	public void setHdfsResultDir(String hdfsResultDir) {
		this.hdfsResultDir = hdfsResultDir;
	}
	/**
	 * @return excelDir
	 */
	
	public String getExcelDir() {
		return excelDir;
	}
	/**
	 * @param excelDir 要设置的 excelDir
	 */
	
	public void setExcelDir(String excelDir) {
		this.excelDir = excelDir;
	}
	/**
	 * @return resultNorm
	 */
	
	public String getResultNorm() {
		return resultNorm;
	}
	/**
	 * @param resultNorm 要设置的 resultNorm
	 */
	
	public void setResultNorm(String resultNorm) {
		this.resultNorm = resultNorm;
	}
	/**
	 * @return isConverId
	 */
	
	public int getIsConverId() {
		return isConverId;
	}
	/*
	 * 是否需要把结果文件中的ID转换成中文名称
	 * 0：需要
	 * 1：不需要
	 */
	public boolean isConverId() {
		return isConverId == 0 ? true :false;
	}
	/**
	 * @param isConverId 要设置的 isConverId
	 */
	
	public void setIsConverId(int isConverId) {
		this.isConverId = isConverId;
	}
	
	
}
