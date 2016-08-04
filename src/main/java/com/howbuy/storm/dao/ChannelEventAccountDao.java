/**
 * 
 */
package com.howbuy.storm.dao;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.howbuy.storm.common.ConnectionDB;
import com.howbuy.storm.dto.ChannelEventAccount;

/**
 * @author qiankun.li
 * 
 */
public class ChannelEventAccountDao implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 7394640469396721804L;

	private Logger logger = LoggerFactory.getLogger(ChannelEventAccountDao.class);

	private ConnectionDB connectionDB = new ConnectionDB();

	private final StringBuffer insertSql = new StringBuffer(
			"INSERT INTO channel_event_account (proid,outletcode,activate_num,openacc_num,bindcard_num,order_num,order_amount,create_time)"
					+ " VALUES ");
	
	private final String h5_event_insertSql ="INSERT INTO channel_event_account_h5 (channel,openacct_num,channel_type,channel_parent,channel_level,create_time)" + " VALUES ";
	
	private final String h5_pv_insertSql ="INSERT INTO channel_pv_account_h5 (channel,pv,uv,enter,hongbao_uv,kaihu_uv,auth_uv,kaihu_result_uv,channel_type,channel_parent,channel_level,create_time)" + " VALUES ";
	
	private final String h5_pv_event_insertSql ="INSERT INTO channel_pv_event_h5 (channel,pv,uv,enter,hongbao_uv,kaihu_uv,auth_uv,kaihu_result_uv,openacct_num,channel_type,channel_parent,channel_level,create_time)" + " VALUES ";

	/**
	 * 添加流量数据入库
	 * @param objectList
	 * @param proidType
	 * @return
	 */
	public int insertList(List<ChannelEventAccount> objectList) {
		if (objectList.size() == 0)
			return -1;
		StringBuffer sqlBf = new StringBuffer(insertSql);
		List<Object[]> insertList = new ArrayList<Object[]>(objectList.size());
		for (ChannelEventAccount record : objectList) {
			String proid = record.getProid();
			String outletcode = record.getOutletcode();
			long activate = record.getActivateNum();
			long openacc = record.getOpenaccNum();
			long bindcard = record.getBindcardNum();
			long order = record.getOrderNum();
			BigDecimal orderAmount = record.getOrderAmount();
			Timestamp createTime = record.getCreateTime();
			Object[] params = { proid, outletcode, activate, openacc, bindcard,order,orderAmount,createTime };
			insertList.add(params);
			sqlBf.append("(?,?,?,?,?,?,?,?)");
			sqlBf.append(",");
		}
		String sqlInsert = sqlBf.toString();
		sqlInsert = sqlInsert.substring(0, sqlInsert.length() - 1);
		logger.debug("sql:{}",sqlInsert);
		int i = connectionDB.executeUpdate(sqlInsert, insertList);
		logger.info("insertList executeUpdate num is " + i);
		return i;
	}
	
	
	/**
	 * H5pv实时数据
	 * @param paramList
	 * @return
	 */
	public int insertH5pv(List<Object[]> paramList) {
		
		if (paramList.size() == 0)
			return -1;
		
		return doInsert(h5_pv_insertSql,paramList,12);
		
	}
	
	/**
	 * H5pv实时数据
	 * @param objectList
	 * @return
	 */
	public int insertH5event(List<Object[]> paramList) {
		
		if (paramList.size() == 0)
			return -1;
		
		return doInsert(h5_event_insertSql,paramList,6);
	}
	
	/**
	 * H5页面pv，event数据
	 */
	public int insertH5pvAndEvent(List<Object[]> paramList){
		
		if(paramList.size() == 0)
			return -1;
			
		return doInsert(h5_pv_event_insertSql,paramList,13);
	}
	
	/**
	 * 通用数据库插入
	 * @param sql
	 * @param paramList
	 * @return
	 */
	private int doInsert(String sql,List<Object[]> paramList,int len){
		
		
		if (paramList.size() == 0)
			return -1;
		
		StringBuilder paramHolderBuilder = new StringBuilder();
		
		paramHolderBuilder.append("(");
		
		for(int i = 0; i < len; i++){
			
			paramHolderBuilder.append("?,");
		}
		
		paramHolderBuilder.deleteCharAt(paramHolderBuilder.length()-1);
		
		paramHolderBuilder.append(")");
		
		String paramHolder = paramHolderBuilder.toString();
		
		StringBuffer sqlBf = new StringBuffer(sql);
		
		for(int i = 0; i < paramList.size(); i++){
			
			sqlBf.append(paramHolder);
			sqlBf.append(",");
		}
		
		String sqlInsert = sqlBf.toString();
		sqlInsert = sqlInsert.substring(0, sqlInsert.length() - 1);
		logger.debug("sql:{}",sqlInsert);
		int i = connectionDB.executeUpdate(sqlInsert, paramList);
		logger.info("insertList executeUpdate num is " + i);
		return i;
	}
}
