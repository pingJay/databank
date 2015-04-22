/**
 * @Title: JobOptionsParse.java
 * @Package com.uniclick.databank.mapreduce.util
 * @Description: TODO(用一句话描述该文件做什么)
 * @author ping.jie
 * @date 2015-4-13 下午6:33:38
 * @version V1.0
 */

package com.uniclick.databank.mapreduce.util;

import com.alibaba.fastjson.JSONObject;
import com.uniclick.databank.mapreduce.pojo.MapReduceJobOptionModel;

/**
 * @ClassName: JobOptionsParse
 * @Description: TODO(MR 任务参数解析)
 * @author ping.jie
 * @date 2015-4-13 下午6:33:38
 * @verision $Id
 * 
 */

public class JobOptionsParse {

	public static MapReduceJobOptionModel parse(String jobOptins) {
		return JSONObject.parseObject(jobOptins,MapReduceJobOptionModel.class);
	}
	
	public static void main(String[] args) {
		String jsonString = "{\"order\":[\"1372\"],\"site\":[\"109\",\"73\",\"55\",\"21\"],\"inventory\":[\"2000\",\"1988\",\"1981\",\"1971\"],\"frequency\":[\"5\"],\"collect\":[\"1\"]}";
		System.out.println(jsonString);
		MapReduceJobOptionModel o = JSONObject.parseObject(jsonString,MapReduceJobOptionModel.class);
		System.out.println(o.getOrder());
		System.out.println(o.getSite());
		System.out.println(o.getCoincide());
	}
}
