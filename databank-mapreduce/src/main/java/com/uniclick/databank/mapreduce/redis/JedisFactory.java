package com.uniclick.databank.mapreduce.redis;



import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.pool.impl.GenericObjectPool;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
/**
 * 连接redis的工厂类
 * @author ping jie
 *
 */
public class JedisFactory {
	
	private GenericObjectPool config;
	private JedisPoolConfig jedisPoolConfig;
	
	private Map<String,JedisPool> map = new HashMap<String,JedisPool>();

	private List<String> host = new ArrayList<String>();
	
	
	public JedisFactory(JedisPoolConfig jedisPoolConfig) {
		super();
		this.jedisPoolConfig = jedisPoolConfig;
	}
	
	public Jedis getJedisInstance(String host,int port) {
		return getJedisPool(host,port).getResource();
	}

	/**
	 * 根据端口数量 创建相应个数的池 
	 * @param hosts 多有redis端口的集�?
	 */
	public void addJedisPoolToMap(String[] hosts) {
		for(int i=0;i<hosts.length;i++) {
			String host = hosts[i];
			JedisPool jedisPool = new JedisPool(jedisPoolConfig, host.split(":")[0], Integer.parseInt(host.split(":")[1]),400000000);
			host = hosts[i];
			map.put(host, jedisPool);
		}
	}
	
	
	public JedisPool getJedisPool(String host,int port) {
			return map.get(host +":"+ port);
	}
	
	/**
	 * 配合使用getJedisInstance方法后将jedis对象释放回连接池�?
	 * 
	 * @param jedis 使用完毕的Jedis对象
	 * @return true 释放成功；否则返回false
	 */
	public boolean release(Jedis jedis,String host,int port) {
		if (jedis != null) {
				map.get(host +":"+ port).returnResource(jedis);
				return true;
		}
		return false;
	}

}
