/**
 * @Title: UTCustomReportTable.java
 * @Package com.uniclick.databank.ut.persist
 * @Description: TODO(用一句话描述该文件做什么)
 * @author ping.jie
 * @date 2015-4-9 下午3:25:08
 * @version V1.0
 */

package com.uniclick.databank.service.ut.persist;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uniclick.databank.service.common.ConfigReader;
import com.uniclick.databank.service.common.Constants;
import com.uniclick.databank.service.common.DbBean;
import com.uniclick.databank.service.ut.pojo.TblCustomReportJob;


/**
 * @ClassName: UTCustomReportTable
 * @Description: TODO(UT的自定义报告相关数据库操作)
 * @author ping.jie
 * @date 2015-4-9 下午3:25:08
 * @verision $Id
 * 
 */

public class UTCustomReportTable {
	private static final Logger logger = LoggerFactory.getLogger(UTCustomReportTable.class);
	private ConfigReader resultNromPorp = new ConfigReader(Constants.RESULTNORM_PROP);
	private DbBean dbBean;
	
	public UTCustomReportTable(DbBean dbBean) {
		this.dbBean = dbBean;
	}
	
	/**
	 * 
	 * @Title: getExectoryJobId
	 * @Description: TODO(查询待执行任务ID 列表)
	 * @param @return    设定文件
	 * @return List<Integer>    返回类型
	 * @throws
	 */
	public List<Integer> getExectoryJobId() {
		List<Integer> ids = new ArrayList<Integer>();
		String sql = "SELECT id FROM tbl_custom_report_job WHERE  status = 0 ";
		try {
			dbBean.setPrepareStatement(sql);
			logger.debug("exec select : " + sql);
			ResultSet result = dbBean.executeQuery();
			while(result.next()) {
				
				ids.add(result.getInt(1));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return ids;
	}
	
	/**
	 * 
	 * @Title: getExectoryJob
	 * @Description: TODO(查询待执行的JOB 对象列表)
	 * @param @return    设定文件
	 * @return List<TblCustomReportJob>    返回类型
	 * @throws
	 */
	public List<TblCustomReportJob> getExectoryJobs() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String nowTime = sdf.format(new Date());
		List<TblCustomReportJob> customJobList = new ArrayList<TblCustomReportJob>();
		String sql = "select JobType , IDCollection , Metrics , StartDate , EndDate , " +
				"MailTo , MailCc , MailTitle , MailContent , Status ,ID ,JobName " +
				"FROM tbl_custom_report_job WHERE  status = 0 and begintime <= ? ";
		try {
			dbBean.setPrepareStatement(sql);
			/*
			 * 此条件是为了过滤，设置的定时执行时间 晚于当前时间的JOB，暂时不执行
			 */
			dbBean.setString(1, nowTime);
			logger.debug("exec select : " + sql);
			ResultSet result = dbBean.executeQuery();
			TblCustomReportJob customJob;
			while(result.next()) {
				customJob = new TblCustomReportJob();
				customJob.setJobType(result.getInt(1));
				customJob.setIdCollection(result.getString(2));
				customJob.setMetrics(resultNromPorp.getPro(result.getString(3).trim().split(",")));
				customJob.setStartDate(result.getString(4));
				customJob.setEndDate(result.getString(5));
				customJob.setMailTo(result.getString(6));
				customJob.setMailCc(result.getString(7));
				customJob.setMailTitle(result.getString(8));
				customJob.setMailContent(result.getString(9));
				customJob.setStatus(result.getInt(10));
				customJob.setId(result.getInt(11));
				customJob.setJobName(result.getString(12));
				customJobList.add(customJob);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return customJobList;
	}
	
	/**
	 * 
	 * @Title: setJobStatus
	 * @Description: TODO(更新JOB状态)
	 * @param @param id
	 * @param @param status
	 * @param @return    设定文件
	 * @return boolean    返回类型
	 * @throws
	 */
	public boolean updateJobStatus(int id,int status) {
		String sql = "UPDATE tbl_custom_report_job SET Status = ? WHERE ID = ?";
		try {
			dbBean.setPrepareStatement(sql);
			dbBean.setInt(1, status);
			dbBean.setInt(2, id);
			if(dbBean.executeUpdate() > 0) {
				return true;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}
	
	/**
	 * 
	 * @Title: cleanProblemJob
	 * @Description: TODO(将今天之前的开始运行且还没有运行完的任务状态修改为失败)
	 * @param @return    设定文件
	 * @return boolean    返回类型
	 * @throws
	 */
	public boolean cleanProblemJob() {
		String sql = "UPDATE `tbl_custom_report_job` set `Status`=? where `Status`=? and DATE_FORMAT(CreatedAt,'%Y-%m-%d') < DATE_FORMAT(NOW(),'%Y-%m-%d')";
		try {
			dbBean.setPrepareStatement(sql);
			dbBean.setInt(1, Constants.JOB_STATUS_FAILED);
			dbBean.setInt(2, Constants.JOB_STATUS_STARTJOB);
			if(dbBean.executeUpdate() > 0) {
				return true;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}
	/**
	 * 
	 * @Title: getRunningJobNum
	 * @Description: TODO(获取当前运行中的任务数量)
	 * @param @return    设定文件
	 * @return int    返回类型
	 * @throws
	 */
	public int getRunningJobNum() {
		int runningNum = 0;
		String sql = "select id from tbl_custom_report_job where Status = ?";
		try {
			dbBean.setPrepareStatement(sql);
			dbBean.setInt(1, Constants.JOB_STATUS_STARTJOB);
			ResultSet resultSet = dbBean.executeQuery();
			while(resultSet.next()) {
				runningNum++;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return runningNum;
	}
	
	public boolean updateExcelPath(TblCustomReportJob vo) {
		String sql = "update tbl_custom_report_job set FilePath = ? where ID = ?";
		try {
			dbBean.setPrepareStatement(sql);
			dbBean.setString(1, vo.getExcelPath());
			dbBean.setInt(2, vo.getId());
			if(dbBean.executeUpdate() > 0){
				return true;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}
}
