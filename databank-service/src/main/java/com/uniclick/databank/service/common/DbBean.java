/**
 * @Title: DbBean.java
 * @Package com.uniclick.dapa.common.dbc
 * @Description: TODO(用一句话描述该文件做什么)
 * @author ping.jie
 * @date 2014年6月11日 下午2:03:19
 * @version V1.0
 */

package com.uniclick.databank.service.common;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

import com.uniclick.excelbuilder.common.Constants;



/**
 * @ClassName: DbBean
 * @Description: TODO(这里用一句话描述这个类的作用)
 * @author ping.jie
 * @date 2014年6月11日 下午2:03:19
 * @verision $Id
 * 
 */

public class DbBean {

    private static Logger logger = Logger.getLogger(DbBean.class);

    /**
     * 数据库连接属性（不适用连接池）。
     */
    private String driver = null; // 数据库驱动
    private String dbUrl = null; // 数据库连接
    private String user = null; // 数据库用户名
    private String password = null; // 数据库密码

    private Connection conn = null;
    private Statement stmt = null;
    private PreparedStatement preStmt = null;
    private ConfigReader configReader = new ConfigReader("jdbc.properties");
    /**
     * isSucced 用来判断连接成功.
     */
    private boolean isSucceed = true;

    public boolean isSucceed() {
        return this.isSucceed;
    }

    public DbBean() {
        if (false == connectDB()) {
            this.isSucceed = false;
            this.conn = null;
        }
    }

    public DbBean(String dirver,String url,String username,String pwd ) {
    	if (false == connectDB(dirver,url,username,pwd)) {
            this.isSucceed = false;
            this.conn = null;
        }
    }
    public boolean connectDB() {
        boolean returnValue = false;
        if (this.conn == null) {
            try {
            	this.driver = configReader.getPro(Constants.JDBC_DRIVER);
            	this.dbUrl = configReader.getPro(Constants.JDBC_UT_ADSURL);
            	this.user = configReader.getPro(Constants.JDBC_UT_USERNAME);
            	this.password = configReader.getPro(Constants.JDBC_UT_PASSWORD);
                Class.forName(this.driver).newInstance();
                this.conn = DriverManager.getConnection(this.dbUrl, this.user,
                        this.password);
                returnValue = true;
            } catch (SQLException e) {
            	e.printStackTrace();
            	logger.error("Can't connect database using pool."
                        + e.getMessage());
            } catch (Exception e) {
            	logger.error("Unknown exception." + e.getMessage());
            }
        }

        return returnValue;
    }

    public boolean connectDB(String driver,String url,String username,String pwd) {
        boolean returnValue = false;
        if (this.conn == null) {
            try {
            	this.driver = configReader.getPro(driver);
            	this.dbUrl = configReader.getPro(url);
            	this.user = configReader.getPro(username);
            	this.password = configReader.getPro(pwd);
                Class.forName(this.driver).newInstance();
                this.conn = DriverManager.getConnection(this.dbUrl, this.user,
                        this.password);
                returnValue = true;
            } catch (SQLException e) {
            	e.printStackTrace();
            	logger.error("Can't connect database using pool."
                        + e.getMessage());
            } catch (Exception e) {
            	logger.error("Unknown exception." + e.getMessage());
            }
        }

        return returnValue;
    }
    /**
     * 设置事务执行状态值
     * 
     * @throws SQLException
     *             捕获异常
     */
    public boolean setAutoCommit(boolean f) {
        boolean b = false;
        try {
            b = this.conn.getAutoCommit();
            this.conn.setAutoCommit(f);
        } catch (SQLException e) {
        	logger.error("数据库操作错误:[" + e.getErrorCode() + "]"
                    + e.getMessage());
        }

        return b;
    }

    /**
     * 取得事务执行状态值
     * 
     * @return boolean 状态值
     * @throws SQLException
     *             捕获异常
     */
    public boolean getAutoCommit() {
        boolean b = false;
        try {
            b = this.conn.getAutoCommit();
        } catch (SQLException e) {
        	logger.error("数据库操作错误:[" + e.getErrorCode() + "]"
                    + e.getMessage());
        }
        return b;
    }

    /**
     * 开始事务
     * 
     * @throws SQLException
     *             捕获异常
     */
    public void beginTrans() {
        // Empty
    }

    /**
     * 取得连接
     * 
     * @return conn Connection 返回连接
     */
    public Connection getConnection() {
        return this.conn;
    }

    /**
     * 数据事务执行
     * 
     * @throws SQLException
     *             捕获异常
     */
    public void commit() throws SQLException {
        this.conn.commit();
    }

    /**
     * 数据事务回滚
     */
    public void rollback() {
        try {
            this.conn.rollback();
        } catch (SQLException e) {
        	logger.error("数据库操作错误:[" + e.getErrorCode() + "]"
                    + e.getMessage());
        }
    }

    /**
     * 清除PrepareStatement中的参数
     * 
     * @throws SQLException
     *             SQL异常
     */
    public void clearParameters() throws SQLException {
        if (null != this.preStmt) {
            this.preStmt.clearParameters();
        }
    }

    /**
     * 设置PrepareStatement
     * 
     * @return PreparedStatement
     */
    public PreparedStatement pstmt(String query) throws SQLException {
        return this.conn.prepareStatement(query);
    }

    /**
     * 设置string值
     * 
     * @param index
     *            序号
     * @param value
     *            string值
     * @throws SQLException
     *             SQL异常
     */
    public void setString(int index, String value) throws SQLException {
        this.preStmt.setString(index, value);
    }

    /**
     * 设置int值
     * 
     * @param index
     *            序号
     * @param value
     *            int值
     * @throws SQLException
     *             SQL异常
     */
    public void setInt(int index, int value) throws SQLException {
        this.preStmt.setInt(index, value);
    }

    /**
     * 设置Boolean值
     * 
     * @param index
     *            序号
     * @param value
     *            Boolean值
     * @throws SQLException
     *             SQL异常
     */
    public void setBoolean(int index, boolean value) throws SQLException {
        this.preStmt.setBoolean(index, value);
    }

    /**
     * 设置Date值
     * 
     * @param index
     *            序号
     * @param value
     *            Date值
     * @throws SQLException
     *             SQL异常
     */
    public void setDate(int index, Date value) throws SQLException {
        this.preStmt.setString(index, value.toString());
    }

    /**
     * 设置Long值
     * 
     * @param index
     *            序号
     * @param value
     *            Long值
     * @throws SQLException
     *             SQL异常
     */
    public void setLong(int index, long value) throws SQLException {
        this.preStmt.setLong(index, value);
    }

    /**
     * 设置Float值
     * 
     * @param index
     *            序号
     * @param value
     *            Float值
     * @throws SQLException
     *             SQL异常
     */
    public void setFloat(int index, float value) throws SQLException {
        this.preStmt.setFloat(index, value);
    }

    /**
     * 设置Bytes值
     * 
     * @param index
     *            序号
     * @param value
     *            Bytes值
     * @throws SQLException
     *             SQL异常
     */
    public void setBytes(int index, byte[] value) throws SQLException {
        this.preStmt.setBytes(index, value);
    }

    /**
     * 设置PrepareStatement并清空参数列表
     * 
     * @param sql
     *            sql表达式
     * @throws SQLException
     *             SQL异常
     */
    public void setPrepareStatement(String sql) throws SQLException {
        this.clearParameters();
        this.preStmt = this.conn.prepareStatement(sql);
    }

    /**
     * 查询数据
     * 
     * @param sql
     *            查询字符串
     * @return ResultSet
     * @throws SQLException
     *             捕获异常
     */
    public ResultSet executeQuery(String sql) throws SQLException {
        ResultSet rs = null;

        this.stmt = this.conn.createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        rs = this.stmt.executeQuery(sql);

        return rs;
    }

    /**
     * 修改数据
     * 
     * @param sql
     *            查询字符串
     * @throws SQLException
     *             捕获异常
     */
    public int executeUpdateReturnRid(String sql) throws SQLException {
        ResultSet rs = null;
        int i = 0;
        try {
            this.stmt = this.conn.createStatement(
                    ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);
            this.stmt.executeUpdate(sql);
            rs = this.stmt.executeQuery("SELECT LAST_INSERT_ID()");
            rs.first();
            i = rs.getInt(1);
        } catch (SQLException e) {
        	logger.error( "dbTrans.executeUpdate:" + e.getMessage(), e);
        }

        return i;
    }

    public ResultSet executeQuery() throws SQLException {
    	return this.preStmt.executeQuery();
    }
    
    
    public int executeUpdate() throws SQLException {
    	return this.preStmt.executeUpdate();
    }
    /**
     * 修改数据
     * 
     * @param sql
     *            查询字符串
     * @throws SQLException
     *             捕获异常
     */
    public int executeUpdate(String sql) throws SQLException {
        this.stmt = this.conn.createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
        return this.stmt.executeUpdate(sql);
    }


    /**
     * 关闭stmt,preStmt and conn.
     * 
     * @throws SQLException
     *             SQL异常
     */
    public void close() {
        try {
            if (this.stmt != null) {
                this.stmt.close();
                this.stmt = null;
            }
            if (this.preStmt != null) {
                this.preStmt.close();
                this.preStmt = null;
            }
            if (this.conn != null) {
                this.conn.close(); // 释放连接
                // this.conn = null; // 不需要设为null
            }
        } catch (SQLException e) {
        	e.printStackTrace();
        	logger.error("数据库操作错误:[" + e.getErrorCode() + "]"
                    + e.getMessage());
        }
    }

}

