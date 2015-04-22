package com.uniclick.databank.mapreduce.common;

/**
 * HADOOP 系统变量
 * 
 * @author kevin wang
 */
public class Constants {

	//--------------------------------------SYSTEM Begin--------------------------------------//
	/**
	 * SYS_PATH_RUN_JOB_JAR
	 */
	public static final String SYS_PATH_RUN_JOB_JAR = "hdfs://namenode/user/pingjie/databank-mapreduce-2.0.0.jar";
	//--------------------------------------SYSTEM End--------------------------------------//
	//--------------------------------------HADOOP Conf Begin--------------------------------------//
	/**
	 * HDFS_SERVER_IP
	 */
	public static final String HDFS_SERVER_HOST = "namenode";
	/**
	 * HDFS_FS_PORT
	 */
	public static final String HDFS_FS_PORT = "8020";
	/**
	 * HDFS_MAPRED_PORT
	 */
	public static final String HDFS_MAPRED_PORT = "8021";
	/**
	 * JOB_CONF_REDUCER_NUMBER
	 */
	public static final int JOB_CONF_REDUCER_NUMBER = 14;
	
	public static final String JOB_QUEUE = "queue3";
	//--------------------------------------HADOOP Conf End--------------------------------------//

	//--------------------------------------BIZ Begin--------------------------------------//
	/**
	 * BIZ_DAP_DOMAIN_PCIP
	 */
	public static final String BIZ_DAP_DOMAIN_PC = "pagechoice.net";
	/**
	 * BIZ_DAP_DOMAIN_GTIP
	 */
	public static final String BIZ_DAP_DOMAIN_GT = "gentags.net";
	/**
	 * BIZ_CONF_DAP_DOMAIN_PCIP
	 */
	public static final String BIZ_CONF_DAP_DOMAIN_PCIP = "biz.conf.dap.domain.pcip";
	/**
	 * BIZ_CONF_DAP_DOMAIN_GTIP
	 */
	public static final String BIZ_CONF_DAP_DOMAIN_GTIP = "biz.conf.dap.domain.gtip";
	/**
	 * BIZ_CONF_DAP_MOTU_SITEIDS
	 */
	public static final String BIZ_CONF_DAP_MOTU_SITEIDS = "biz.conf.dap.motu.siteids";
	/**
	 * BIZ_FLAG_DAP_PRODUCT_DAP
	 */
	public static final String BIZ_FLAG_DAP_PRODUCT_DAP = "dap";
	/**
	 * BIZ_FLAG_DAP_PRODUCT_MOTU
	 */
	public static final String BIZ_FLAG_DAP_PRODUCT_MOTU = "motu";
	/**
	 * BIZ_FLAG_DAP_PRODUCT_DSP
	 */
	public static final String BIZ_FLAG_DAP_PRODUCT_DSP = "dsp";
	//--------------------------------------BIZ End--------------------------------------//
	
	//--------------------------------------UT LOG FIELDS-----------------------------------------------//
	public static final String  UT_LOG_LOGTYPE = "logType";
	public static final String  UT_LOG_HOST= "host";
	public static final String UT_LOG_INVENTORYID = "inventoryId";
	public static final String UT_LOG_ORDERITEMID = "orderItemId";
	public static final String UT_LOG_SITEID = "siteId";
	public static final String UT_LOG_CREATETIVEID = "createtiveId";
	public static final String UT_LOG_COOKIEID = "cookieId";
	public static final String UT_LOG_IP = "ip";
	public static final String UT_LOG_ADVID = "advId";
	public static final String UT_LOG_ORDERID = "orderId";
	public static final String UT_LOG_CREATETIME = "createTime";
	public static final String UT_LOG_CITY = "city";
	public static final String UT_LOG_REGION = "region";
	public static final String UT_LOG_COUNTRY = "country";
	public static final String UT_LOG_REFERENCE = "reference";
	public static final String UT_LOG_URL = "url";
	public static final String UT_LOG_INVENRTORYTAG = "inventoryTag";
	public static final String UT_LOG_INVENTORYNAME = "inventoryName";
	public static final String UT_LOG_TIP = "t_ip";
	public static final String UT_LOG_ISMOBILE = "ismobile";
	public static final String UT_LOG_PHONETYPE = "phonetype";
	public static final String UT_LOG_WEBBROWSER = "webbrowser";
	public static final String UT_LOG_OS = "os";
	public static final String UT_LOG_UA = "ua";
	public static final String UT_LOG_VISITORCUSTOMVAR = "visitorCustomVar";
	public static final String UT_LOG_GOALSTR = "goalStr";
	public static final String UT_LOG_BROWSERPLUGIN = "browserPlugin";
	public static final String UT_LOG_SCREENSIZE = "screenSize";
	public static final String UT_LOG_ISP = "ISP";
	public static final String UT_LOG_URI = "uri";
	public static final String UT_LOG_PAGECUSTOMVAR = "pageCustomVar";
	public static final String UT_LOG_TITLE = "title";
	public static final String UT_LOG_CLKCOOKIEADIDS = "clkCookieAdIds";
	public static final String UT_LOG_SITECOOKIEIDS = "siteCookieIds";
}
