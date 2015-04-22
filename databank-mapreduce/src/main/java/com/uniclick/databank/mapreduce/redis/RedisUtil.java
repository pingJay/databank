package com.uniclick.databank.mapreduce.redis;

import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;


/**
 * @ClassName: RedisUtil
 * @Description: TODO(redis工具类，提供redis的一些基本查询，增加等功能)
 * @author: ping.jie
 * @date: 2012-11-2   上午10:37:41 
 * @revision          $Id
 */
public class RedisUtil {

	private static JedisFactory factory = new JedisFactory(new JedisPoolConfig());
	//private static ResourceBundle rb = ResourceBundle.getBundle("Host"); //读取属性文件
	private static ConsistentHash ch;
	
	static {
		//String[] hosts = rb.getString("host").split(","); //读取文件中属性，host为属性名称
		String[] hosts = {"172.16.213.79:6379","172.16.213.79:6380","172.16.213.79:6381"
				,"172.16.213.79:6382","172.16.213.80:6379","172.16.213.80:6380"
				,"172.16.213.80:6381","172.16.213.80:6382","172.16.213.81:6379"
				,"172.16.213.81:6380","172.16.213.81:6381","172.16.213.81:6382"
				,"172.16.213.82:6379","172.16.213.82:6380","172.16.213.82:6381",
				"172.16.213.82:6382","172.16.213.83:6379","172.16.213.83:6380"
				,"172.16.213.83:6381","172.16.213.83:6382","172.16.213.84:6379"
				,"172.16.213.84:6380","172.16.213.84:6381","172.16.213.84:6382"};
		factory.addJedisPoolToMap(hosts); //根据host 数量准备好 JedisPool 
		ch = new ConsistentHash(new HashFunction(),64,hosts); //计算一致性hash对象
	}
	
	public static Set<String> hKeys(String cookies) {
		String[] ip_part = ch.get(cookies).split(":");
		String ip = ip_part[0];
		int port = Integer.parseInt(ip_part[1]);
		Jedis jedis = factory.getJedisInstance(ip,port);
		Set<String> crowds = jedis.hkeys(cookies); //查出这个cookie的所有的属性
		factory.release(jedis, ip, port);
		return crowds;
	}
	
	public static Set<String> hKeys(String cookies,int dbNum) {
		String[] ip_part = ch.get(cookies).split(":");
		String ip = ip_part[0];
		int port = Integer.parseInt(ip_part[1]);
		Jedis jedis = factory.getJedisInstance(ip,port);
		jedis.select(dbNum);
		Set<String> crowds = jedis.hkeys(cookies); //查出这个cookie的所有的属性
		factory.release(jedis, ip, port);
		return crowds;
	}
	
	public static String KeysToString(String cookies) {
		StringBuffer sb = new StringBuffer();
		Set<String> crowds = hKeys(cookies);
		for(String crowd : crowds) {
			sb.append(crowd).append(";");
		}
		String strCrowds = sb.toString();
		if(strCrowds.length() >= 1) {
			strCrowds = strCrowds.substring(0, strCrowds.length()-1);
		}
		return strCrowds;
	}
	
	public static void main(String args[]) {
		System.out.println(ch.get("fd3e943-505ff8480c7cd6-64018473"));
	}
}

