/**
 * 
 */
package com.howbuy.storm.test;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.howbuy.storm.common.ConnectionDB;
import com.howbuy.storm.dao.ChannelEventAccountDao;
import com.howbuy.storm.dao.ChannelPagePvUvDao;
import com.howbuy.storm.dto.ChannelPageRecord;

/**
 * @author qiankun.li
 *
 */
public class ConTest {

	
	public static void main(String[] args) {
		ConnectionDB connectionDB = new ConnectionDB();
		ChannelPagePvUvDao pvUvDao = new ChannelPagePvUvDao();
		ChannelEventAccountDao accountDao = new ChannelEventAccountDao();
		/*final StringBuffer insertSql = new StringBuffer("INSERT INTO zero_channel_tag (tag_name,tag_code,createDate,type,title,z_order)"
				+ " VALUES ");
		List<Object[]> list1 = new ArrayList<Object[]>();
		for(int  i=0;i<30000;i++){
			String tagName = "tagName"+i;
			String tagCode = "tag_code"+i;
			Date createDate = new Date();
			int type =1;
			String title = "title"+i;
			int z_order = 1;
			Object[] params = {tagName,tagCode,createDate,type,title,z_order};
			list1.add(params);
			insertSql.append("(?,?,?,?,?,?)");
			insertSql.append(",");
		}
		String sqlInsert = insertSql.toString();
		
		sqlInsert = sqlInsert.substring(0,sqlInsert.length()-1);
		System.out.println(sqlInsert);
		int e = connectionDB.executeUpdate(sqlInsert, list1);
		System.out.println("执行insert�?��["+e+"]条记�?);*/
		
		List<ChannelPageRecord> list = new ArrayList<ChannelPageRecord>();
		for(int j = 0;j<2;j++){
			ChannelPageRecord record = new ChannelPageRecord();
			record.setCreatetime(new Timestamp(System.currentTimeMillis()));
			record.setDt(new Date(System.currentTimeMillis()));
			record.setPageId("pageid_"+j);
			record.setProid("10012");
			record.setPv(2000);
			record.setUv(1000);
			list.add(record);
		}
		pvUvDao.insertList(list);
		
		
		/*String sql = "SELECT * FROM zero_channel_tag t";
		Object[] params = {};
		List<Map<String, Object>> list = connectionDB.excuteQuery(sql, params);
		if(null!=list){
			for(Map<String, Object> o :list){
				Set<String> keySets=  o.keySet();
				System.out.println();
				for(String col : keySets){
					Object value = o.get(col);
					System.out.print(""+col +","+value +"\t");
				}
				System.out.println();
			}
		}*/
		/*List<ChannelEventAccount> objectList = new ArrayList<ChannelEventAccount>();
		
		for(int i=0;i<2000;i++){
			ChannelEventAccount account = new ChannelEventAccount();
			account.setActivateNum(1);
			account.setBindcardNum(1);
			account.setCreateTime( new Timestamp(System.currentTimeMillis()));
			account.setOpenaccNum(2);
			account.setOrderAmount(new BigDecimal(222.09));
			account.setOrderNum(2);
			account.setOutletcode("9999"+i);
			account.setProid("8888");
			objectList.add(account);
			
		}
		accountDao.insertList(objectList);*/
		
	}
}
