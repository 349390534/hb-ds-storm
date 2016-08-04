/**
 * 
 */
package com.howbuy.storm.h5;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.howbuy.storm.common.HbConstants;

/**
 * @author qiankun.li
 *
 */
public class H5Topology {

	public static void main(String[] args) {
		
		TopologyBuilder builder = new TopologyBuilder(); 
		
		H5PVSpot h5pvSpot = new H5PVSpot();
		
		H5EventSpot h5eventSpot = new H5EventSpot();
		
		builder.setSpout(HbConstants.H5_PV_SPOT_ID, h5pvSpot,3);
		
		builder.setSpout(HbConstants.H5_EVENT_SPOT_ID, h5eventSpot,3);
		
		H5Bolt activationEventBolt = new H5Bolt();
		builder.setBolt("h5Bolt", activationEventBolt,6)
		.fieldsGrouping(HbConstants.H5_PV_SPOT_ID, new Fields("guid"))
		.fieldsGrouping(HbConstants.H5_EVENT_SPOT_ID, new Fields("otrack"));

		builder.setBolt("gatherbolt", new H5GatherBolt(),5)
		.fieldsGrouping("h5Bolt", new Fields("channel"));
		
		Config conf = new Config();
//		LocalCluster cluster = new LocalCluster();
//		conf.setDebug(true);
//		conf.setNumWorkers(2);
//		cluster.submitTopology("AppActivationEvent_Topology", conf, builder.createTopology());
//		try {
//			Thread.sleep(1000 * 60 * 12);
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		cluster.shutdown();
		try {
			conf.setNumWorkers(6);
			StormSubmitter.submitTopology("H5_Topology", conf,builder.createTopology());
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
	}
	
}
