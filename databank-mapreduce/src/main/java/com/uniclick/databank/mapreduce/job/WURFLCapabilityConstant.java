package com.uniclick.databank.mapreduce.job;

/**
 * @ClassName: WURFLCapabilityConstant
 * @Description: TODO(wurfl 获取各种属性的对应的名称)
 * @author: ping.jie
 * @date: 2013-12-13   上午10:08:32 
 * @revision          $Id
 */
public class WURFLCapabilityConstant {
	/*
	 * 是否有一个真实的桌面系统
	 */
	public static final String IS_FULL_DESKTOP = "is_full_desktop";
	/*
	 * 是否为手机
	 */
	public static final String IS_MOBILE = "is_mobile";
	/*
	 * 是否为机器人、爬虫等
	 */
	public static final String IS_ROBOT = "is_robot";
	/*
	 * 是否为平板
	 */
	public static final String IS_TABLET = "is_tablet";
	/*
	 * 操作系统，可以辨别所有的移动设备与PC
	 */
	public static final String ADVERTISED_DEVICE_OS = "advertised_device_os";
	/*
	 * 操作系统的版本。可识别移动设备与PC
	 */
	public static final String ADVERTISED_DEVICE_OS_VERSION = "advertised_device_os_version";
	/*
	 * 浏览器。可识别移动设备与PC
	 */
	public static final String ADVERTISED_BROWSER = "advertised_browser";
	/*
	 * 	浏览器版本。可识别移动设备与PC
	 */
	public static final String ADVERTISED_BROWSER_VERSION = "advertised_browser_version";
	/*
	 * 	是否为触屏
	 */
	public static final String IS_TOUCHSCREEN = "is_touchscreen";
	/*
	 * 输入方式（触屏，鼠标等等）
	 */
	public static final String POINTING_METHOD = "pointing_method";
	/*
	 * 是否app进入的
	 */
	public static final String IS_APP = "is_app";
	/*
	 * 是否为android系统
	 */
	public static final String IS_ANDROID = "is_android";
	/*
	 * 是否为ios系统
	 */
	public static final String IS_IOS = "is_ios";
	/*
	 * Windows Phone 6.5或更高版本。请注意,这并不包括Windows Mobile或Windows CE。
	 */
	public static final String IS_WINDOWS_PHONE = "is_windows_phone";
	/*
	 * 设备型号
	 */
	public static final String DEVICE_ID = "device_id";
}

