/**
 * @Title: UTCustomJobTable.java
 * @Package com.uniclick.databank.service.ut.persist
 * @Description: TODO(用一句话描述该文件做什么)
 * @author ping.jie
 * @date 2015-4-14 下午7:46:53
 * @version V1.0
 */

package com.uniclick.databank.service.ut.persist;


import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uniclick.databank.service.common.ConfigReader;
import com.uniclick.databank.service.common.Constants;
import com.uniclick.databank.service.common.DbBean;
import com.uniclick.databank.service.ut.pojo.TblCustomJobBasic;

/**
 * @ClassName: UTCustomJobTable
 * @Description: TODO(对 tbl_custom_job_basic 表的操作)
 * @author ping.jie
 * @date 2015-4-14 下午7:46:53
 * @verision $Id
 * 
 */

public class UTCustomJobTable {
	private static final Logger logger = LoggerFactory.getLogger(UTCustomJobTable.class);
	private DbBean dbBean;
	private ConfigReader resultNromPorp = new ConfigReader(Constants.RESULTNORM_PROP);
	public UTCustomJobTable(DbBean dbBean) {
		this.dbBean = dbBean;
	}
	
	public TblCustomJobBasic getTblCustomJobBasic(int jobId) {
		TblCustomJobBasic customJobBasic = new TblCustomJobBasic();
		String sql = "select jobName,jobClassName,jobhsql,hdfsResultDir,excelDir,resultNorm,isConvertID from tbl_custom_job_basic" +
				" where id = ?";
		try {
			dbBean.setPrepareStatement(sql);
			dbBean.setInt(1, jobId);
			ResultSet resultSet = dbBean.executeQuery();
			if(resultSet.wasNull()) {
				logger.info("JobID :" + jobId + " is no query to");
				return null;
			}
			customJobBasic.setId(jobId);
			while(resultSet.next()){
				customJobBasic.setJobName(resultSet.getString(1));
				customJobBasic.setJobClassName(resultSet.getString(2));
				customJobBasic.setJobhsql(resultSet.getString(3));
				customJobBasic.setHdfsResultDir(resultSet.getString(4));
				customJobBasic.setExcelDir(resultSet.getString(5));
				customJobBasic.setResultNorm(resultNromPorp.getPro(resultSet.getString(6).trim().split(",")));
				customJobBasic.setIsConverId(resultSet.getInt(7));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return customJobBasic;
	}
}
