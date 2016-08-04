/**
 * 
 */
package com.howbuy.storm.dao;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.howbuy.storm.common.ConnectionDB;
import com.howbuy.storm.dto.ChannelPageRecord;

/**
 * @author qiankun.li
 * 
 */
public class ChannelPagePvUvDao implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 7394640469396721804L;

	private Logger logger = LoggerFactory.getLogger(ChannelPagePvUvDao.class);

	private ConnectionDB connectionDB = new ConnectionDB();

	private final StringBuffer insertSql = new StringBuffer(
			"INSERT INTO channel_page_pv_uv (dt,proid,page_id,pv,uv,create_time)"
					+ " VALUES ");

	/**
	 * 添加流量数据入库
	 * 
	 * @param objectList
	 * @param proidType
	 * @return
	 */
	public int insertList(List<ChannelPageRecord> objectList) {
		if (objectList.size() == 0)
			return -1;
		StringBuffer sqlBf = new StringBuffer(insertSql);
		List<Object[]> insertList = new ArrayList<Object[]>(objectList.size());
		for (ChannelPageRecord record : objectList) {
			Date dt = record.getDt();
			String proid = record.getProid();
			String page_id = record.getPageId();
			long pv = record.getPv();
			long uv = record.getUv();
			Timestamp createTime = record.getCreatetime();
			Object[] params = { dt, proid, page_id, pv, uv,createTime };
			insertList.add(params);
			sqlBf.append("(?,?,?,?,?,?)");
			sqlBf.append(",");
		}
		String sqlInsert = sqlBf.toString();
		sqlInsert = sqlInsert.substring(0, sqlInsert.length() - 1);
		logger.info("sql:{}",sqlInsert);
		int i = connectionDB.executeUpdate(sqlInsert, insertList);
		logger.info("insertList executeUpdate num is " + i);
		return i;
	}
}
