/**
 * 
 */
package com.howbuy.storm.common;

import org.apache.commons.lang.StringUtils;

/**
 * @author qiankun.li
 * 
 */
public abstract class Util {
	
	public static boolean isEmpty(String val) {

		if (StringUtils.isEmpty(val) || "null".equals(val))
			return true;
		return false;
	}
}
