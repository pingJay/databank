package com.uniclick.databank.service.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @ClassName: ConfigReader
 * @Description: TODO(properties 操作类)
 * @author: ping.jie
 * @date: 2013-11-8   下午02:36:07 
 * @revision          $Id
 */
public class ConfigReader {
	private Map<String,String> proInfo ;
	
	public ConfigReader(String propertiesName) {
		proInfo = new HashMap<String,String>();
		java.util.Properties prop = new  java.util.Properties();
		InputStream in;
		in =Thread.currentThread().getContextClassLoader().getResourceAsStream(propertiesName);
		try {
			prop.load(in);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Set<Object> keys = prop.keySet();
		for(Object key : keys) {
			proInfo.put((String)key, prop.getProperty((String)key));
		}
	}
	
	public String getPro(String key) {
		if(proInfo != null && proInfo.containsKey(key)) {
			return proInfo.get(key);
		}
		return key;
	}
	
	public String getPro(String[] keys) {
		StringBuilder sb = new StringBuilder();
		for(int i=0;i<keys.length;i++) {
			if(i < keys.length-1) {
				sb.append(this.getPro(keys[i])).append(",");
			}else{
				sb.append(this.getPro(keys[i]));
			}
		}
		return sb.toString();
	}
	
	public static void main(String[] arhs) {
		System.out.println(ConfigReader.class.getResource("").getPath());
		ConfigReader configReader = new ConfigReader("jdbc.properties");
		System.out.println(configReader.getPro("jdbc.driver"));
	}
}

