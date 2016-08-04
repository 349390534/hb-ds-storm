package com.howbuy.storm.dto;

import java.math.BigDecimal;
import java.sql.Timestamp;

public class ChannelEventAccount {

	/**
	 * 平台号
	 */
	private String proid;
	/**
	 * 网点号
	 */
	private String outletcode;
	/**
	 * 激活人数
	 */
	private int activateNum;
	/**
	 * 开户数量
	 */
	private int openaccNum;
	/**
	 * 绑卡人数
	 */
	private int bindcardNum;
	/**
	 * 下单人数
	 */
	private int orderNum;

	/**
	 * 下单金额
	 */
	private BigDecimal orderAmount;
	/**
	 * 创建时间
	 */
	private Timestamp createTime;

	public String getProid() {
		return proid;
	}

	public void setProid(String proid) {
		this.proid = proid;
	}

	public String getOutletcode() {
		return outletcode;
	}

	public void setOutletcode(String outletcode) {
		this.outletcode = outletcode;
	}

	public int getActivateNum() {
		return activateNum;
	}

	public void setActivateNum(int activateNum) {
		this.activateNum = activateNum;
	}

	public int getOpenaccNum() {
		return openaccNum;
	}

	public void setOpenaccNum(int openaccNum) {
		this.openaccNum = openaccNum;
	}

	public int getBindcardNum() {
		return bindcardNum;
	}

	public void setBindcardNum(int bindcardNum) {
		this.bindcardNum = bindcardNum;
	}

	public int getOrderNum() {
		return orderNum;
	}

	public void setOrderNum(int orderNum) {
		this.orderNum = orderNum;
	}

	public Timestamp getCreateTime() {
		return createTime;
	}

	public void setCreateTime(Timestamp createTime) {
		this.createTime = createTime;
	}

	public BigDecimal getOrderAmount() {
		return orderAmount;
	}

	public void setOrderAmount(BigDecimal orderAmount) {
		this.orderAmount = orderAmount;
	}

}
