/**
 * 
 */
package com.howbuy.storm.common;

/**
 * @author qiankun.li
 * 
 */
public interface HbConstants {
	
	/**
	 *消息分隔符 
	 */
	String FIELD_SEPARATOR = "\u0001";
	
	/**
	 * 休眠间隔4min
	 */
	int SLEEPTIME_MIN =4;
	
	/**
	 * 网站
	 */
	String WEB_PROID = "1001";
	
	/**
	 * 无线
	 */
	String MOBILE_PROID = "5002";
	
	
	String H5_PV_SPOT_ID = "h5pvSpot";
	
	String H5_EVENT_SPOT_ID = "h5eventSpot";
	
	String APP_EVENT = "appevent";
	
	String APP_PV = "apppv";
}
