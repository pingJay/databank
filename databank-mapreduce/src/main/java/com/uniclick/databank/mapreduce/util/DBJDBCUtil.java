package com.uniclick.databank.mapreduce.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;


public class DBJDBCUtil {
//	private static String driver = "org.gjt.mm.mysql.Driver"; // 数据库驱动 old
	private static String driver = "com.mysql.jdbc.Driver"; // 数据库驱动 new
	private static String dbUrl = "jdbc:mysql://172.16.213.36:3306/ads_ds?;useUnicode=true;characterEncoding=utf-8"; // 数据库连接
	private static String user = "ads"; // 数据库用户名
	private static String password = "ads"; // 数据库密码
	private static Connection conn = null;
	
	public static Connection getConnection() {
		try {
			if (conn == null || conn.isClosed()) {
				Class.forName(driver);
				conn = DriverManager.getConnection(dbUrl, user,password);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return conn;
	}
	
	public static void close(){
		if(conn!=null){
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	/**
	 * 广告位ID转化为广告位名称
	 * 有的广告位ID不同，而广告位名称相同
	 */
	public static String  findInventoryNameInventoryId(String keys)  {
		PreparedStatement prestm =null;
		ResultSet cRs = null;
		String value = "";
		if("null".equalsIgnoreCase(keys)) {
			keys = "0";
		}
		try {
			//广告位ID转化为广告位名称
			String crowdsql = "  SELECT InventoryName FROM tbl_inventory WHERE Inventoryid = ?  ";
			prestm = conn.prepareStatement(crowdsql);
			prestm.setString(1, keys);
			cRs = prestm.executeQuery();
			if(cRs.next()) {
				value += cRs.getString(1);
			}else{ 
				return "0";
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return value;
	}

	public static void main(String[] args) {
//		DBJDBCUtil.getConnection();
//		System.out.println(findInventoryNameInventoryId("3890"));
//		DBJDBCUtil.close();
		
		System.out.println(getValue("3890"));
		System.out.println(getValue("3890"));
		System.out.println(getValue("1972"));
		
		
	}
	
	private static Map<String,String> map = new HashMap<String,String>();
	/**
	 * Java Map实现缓存
	 * @param key
	 * @return value
	 */
	public static String getValue(String key){
		String value = map.get(key);
		if(value == null){
			DBJDBCUtil.getConnection();
			value = findInventoryNameInventoryId(key);
			DBJDBCUtil.close();
			map.put(key, value);
		}
		return value;
	}

}
