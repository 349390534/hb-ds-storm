/**
 * 
 */
package com.howbuy.storm.enums;

/**
 * @author qiankun.li
 * 模块id 四位uac_proid值+自定义模块值
 */
public enum AppProidType {
	/**
	 * 2001储蓄罐iPhone APP
	 */
	APP_PROID_CXG_IPHONE("2001","储蓄罐iPhone APP"),
	/**
	 * 2002储蓄罐安卓 APP
	 */
	APP_PROID_CXG_ANDROID("2002","储蓄罐安卓 APP"),
	/**
	 *3001 掌上基金iPhone APP
	 */
	APP_PROID_ZJ_IPHONE("3001","掌上基金iPhone APP"),
	/**
	 * 3002掌上基金安卓 APP
	 */
	APP_PROID_ZJ_ANDROID("3002","掌上基金安卓 APP"),
	;

	private String index;
	private String name;

	private AppProidType(String index, String name) {
		this.index= index;
		this.name= name;
	}

	public String getIndex() {
		return index;
	}

	public void setIndex(String index) {
		this.index = index;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

}
