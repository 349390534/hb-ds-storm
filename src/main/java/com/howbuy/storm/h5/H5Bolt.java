package com.howbuy.storm.h5;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.howbuy.storm.common.HbConstants;

/**
 * @author yichao.song
 * 
 * 汇总pv，event
 */
public class H5Bolt extends BaseRichBolt {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private Logger logger = LoggerFactory.getLogger(getClass());

	private List<String> h5eventlist = new ArrayList<String>();

	private Map<String, List<String>> guid2dataMap = new HashMap<String, List<String>>();

	private static ExecutorService ES = Executors.newFixedThreadPool(2);
	
	private OutputCollector collector;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		
		this.collector = collector;
		

		final H5EventAnalyser h5eventAnaly = new H5EventAnalyser(collector);
		
		final H5PVAnalyser h5pvAnaly = new H5PVAnalyser(collector);
		
		new Thread() {

			@Override
			public void run() {

				while (true) {

					Calendar c = Calendar.getInstance();
					int MINUTE = c.get(Calendar.MINUTE);
					int SECOND = c.get(Calendar.SECOND);

					if ((MINUTE + 1) % 5 == 0 && SECOND == 59) {// 每5分钟触发

						//进位到5分钟整数，取出秒，毫秒
						c.set(Calendar.MINUTE, c.get(Calendar.MINUTE)+1);
						final Timestamp ts = getCt(c);
						
						// 根据otrack分渠道计算开户
						ES.execute(new Runnable() {
							@Override
							public void run() {

								try {
									
									List<String> vals ;

									synchronized (this) {
										
										vals = h5eventlist;
										
										h5eventlist = new ArrayList<String>();

									}
									
									h5eventAnaly.cal(vals, ts);
									
								} catch (Exception e) {
									
									logger.error("event cal:",e);
								}
							}

						});

						// 根据htag分渠道计算pvuv
						ES.execute(new Runnable() {
							@Override
							public void run() {

								try {
									
									Map<String, List<String>> vals ;

									synchronized (this) {
										
										vals = guid2dataMap;
										
										guid2dataMap = new HashMap<String, List<String>>();

									}
									
									h5pvAnaly.cal(vals,ts);
									
								} catch (Exception e) {
									
									logger.error("pv cal:",e);
								}
							}

						});
						
						
						try {
							TimeUnit.MINUTES.sleep(4);
						} catch (InterruptedException e) {
							
						}
					}
				}
			}

		}.start();

	}

	@Override
	public void execute(Tuple input) {
		
		String sourceid = input.getSourceComponent();

		if (HbConstants.H5_EVENT_SPOT_ID.equals(sourceid)) {

			synchronized (this) {

				h5eventlist.add(input.getStringByField("data"));
				
				logger.debug("getStringByField:{}",input.getStringByField("data"));
			}

		} else if (HbConstants.H5_PV_SPOT_ID.equals(sourceid)) {

			String guid = input.getStringByField("guid");

			String data = input.getStringByField("data");
			
			logger.debug("myguid:{}----{}",guid,data);

			synchronized (this) {

				List<String> datas = guid2dataMap.get(guid);

				if (datas == null) {

					datas = new ArrayList<String>();
					
					guid2dataMap.put(guid, datas);
				}

				datas.add(data);

			}
		}
		
		this.collector.ack(input);

	}
	
	/*
	 * 将传入的Calendar秒，毫秒设为0
	 */
	private Timestamp getCt(Calendar now) {
		now.set(Calendar.SECOND, 0);
		now.set(Calendar.MILLISECOND, 0);
		return new Timestamp(now.getTimeInMillis());
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
		declarer.declare(new Fields("source","channel","channeldata"));

	}

}
