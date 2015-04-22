package com.uniclick.databank.mapreduce.job;


import java.net.URI;

import net.sourceforge.wurfl.core.Device;
import net.sourceforge.wurfl.core.GeneralWURFLEngine;
import net.sourceforge.wurfl.core.resource.XMLResource;

/**
 * @ClassName: WURFLUtil
 * @Description: TODO(WURFL 操作工具类)
 * @author: ping.jie
 * @date: 2013-12-12   下午06:02:47 
 * @revision          $Id
 */
public class WURFLUtil {
	private GeneralWURFLEngine gWURFLEngine;
	
	/**
	 * 
	 * @param wurflxmlFilePath  wurfl.xml文件的位置
	 */
	public WURFLUtil(String wurflxmlFilePath) {
		XMLResource xmlResource = new XMLResource(wurflxmlFilePath);
		gWURFLEngine = new GeneralWURFLEngine(wurflxmlFilePath);
	}
	/**
	 * 
	 * @param userAgent  
	 * @param CapabilityName 从WURFLCapabilityConstant类中获取属性对应名称传入
	 * @return 要查询属性的值
	 */
	public String getCapability(String userAgent,String CapabilityName) {
		Device device = gWURFLEngine.getDeviceForRequest(userAgent);
		if(WURFLCapabilityConstant.DEVICE_ID.equals(CapabilityName)) {
			return device.getCapability("brand_name") + " " + device.getCapability("model_name");
		}
		return device.getCapability(CapabilityName);
	}
	
	/**
	 * 
	 * @param userAgent
	 * @return 返回设备类型，用枚举DeviceType表示
	 */
	public DeviceType getDeviceType(String userAgent){
		Device device = gWURFLEngine.getDeviceForRequest(userAgent);
		if(Boolean.parseBoolean(device.getCapability(WURFLCapabilityConstant.IS_FULL_DESKTOP))) {
			return DeviceType.PC;
		}else if(Boolean.parseBoolean(device.getCapability(WURFLCapabilityConstant.IS_TABLET))) {
			return DeviceType.TABLET;
		}else if(Boolean.parseBoolean(device.getCapability(WURFLCapabilityConstant.IS_MOBILE))) {
			return DeviceType.MOBILE;
		}else{
			return DeviceType.OTHER;
		}
	}
	
	public static void main(String[] args) {
		XMLResource xmlResource = new XMLResource(URI.create("hdfs://namenode/user/properties/wurfl.xml"));
		GeneralWURFLEngine gWURFLEngine = new GeneralWURFLEngine(xmlResource);
		Device device =  gWURFLEngine.getDeviceForRequest("Mozilla/5.0 (Windows NT 6.1; WOW64; rv:26.0) Gecko/20100101 Firefox/26.0");
		
	}
}


