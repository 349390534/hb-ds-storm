/**
 * 
 */
package com.howbuy.storm.app;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.howbuy.storm.job.AppActivationAllJob;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

/**
 * @author qiankun.li
 *
 */
public class AppActivationEventAllBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7584011593868349207L;
	
	private  static final Logger LOGGER = LoggerFactory.getLogger(AppActivationEventAllBolt.class);
	

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) { 
		/*if(!AppActivationAllJob.getJob().isStart()){
		}*/
		
		AppActivationAllJob.getJob().excute();
	}

	@Override
	public void execute(Tuple input) {
		String sourceid = input.getSourceComponent();
		String message= input.getStringByField("data");
		LOGGER.debug("AppActivationEventAllBolt receive data :{}",message);
		AppActivationAllJob.getJob().putIntoQueue(message);
		String name = Thread.currentThread().getName();
		if("activationSpot".equals(sourceid)){
			//来自激活的数据流
			LOGGER.debug("Thread :"+name+",input activationSpot data:"+message);
			
		}else if("appEventSpot".equals(sourceid)){
			//来自事件的数据流
			LOGGER.debug("Thread :"+name+",input appEventSpot data:"+message);
		}
	}

}
