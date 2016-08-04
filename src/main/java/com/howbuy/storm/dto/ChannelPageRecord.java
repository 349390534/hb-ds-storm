package com.howbuy.storm.dto;

import java.sql.Date;
import java.sql.Timestamp;

public class ChannelPageRecord {

	private Date dt;

	private String proid;

	private String pageId;

	private long pv;

	private long uv;

	private Timestamp createtime;

	public ChannelPageRecord() {
	}

	public ChannelPageRecord(Date dt, String proid, String pageId, long pv,
			long uv, Timestamp createtime) {
		this.dt = dt;
		this.proid = proid;
		this.pageId = pageId;
		this.pv = pv;
		this.uv = uv;
		this.createtime = createtime;
	}

	public Date getDt() {
		return dt;
	}

	public void setDt(Date dt) {
		this.dt = dt;
	}

	public String getProid() {
		return proid;
	}

	public void setProid(String proid) {
		this.proid = proid;
	}

	public String getPageId() {
		return pageId;
	}

	public void setPageId(String pageId) {
		this.pageId = pageId;
	}

	public long getPv() {
		return pv;
	}

	public void setPv(long pv) {
		this.pv = pv;
	}

	public long getUv() {
		return uv;
	}

	public void setUv(long uv) {
		this.uv = uv;
	}

	public Timestamp getCreatetime() {
		return createtime;
	}

	public void setCreatetime(Timestamp createtime) {
		this.createtime = createtime;
	}

}
