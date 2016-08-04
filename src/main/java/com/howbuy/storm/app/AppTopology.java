/**
 * 
 */
package com.howbuy.storm.app;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * @author qiankun.li
 *
 */
public class AppTopology {

	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder(); 
		AppActivationSpot activationSpot = new AppActivationSpot();
		AppEventSpot appEventSpot = new AppEventSpot();
		builder.setSpout("activationSpot", activationSpot,3);
		builder.setSpout("appEventSpot", appEventSpot,3);
		
		AppActivationEventBolt activationEventBolt = new AppActivationEventBolt();
		builder.setBolt("appActivationEventBolt", activationEventBolt,3)
		.fieldsGrouping("activationSpot",new Fields("proid","outletcode"))
		.fieldsGrouping("appEventSpot",new Fields("proid","outletcode"));
		
		AppActivationEventAllBolt activationEventAllBolt = new AppActivationEventAllBolt();
		builder.setBolt("appActivationEventAllBolt", activationEventAllBolt,3)
		.fieldsGrouping("activationSpot", new Fields("proid"))
		.fieldsGrouping("appEventSpot",new Fields("proid"));
		
		
		Config conf = new Config();
		/*LocalCluster cluster = new LocalCluster();
		conf.setDebug(true);
		cluster.submitTopology("AppActivationEvent_Topology", conf, builder.createTopology());
		Thread.sleep(10000);
		cluster.shutdown();*/
		try {
			conf.setNumWorkers(6);
			StormSubmitter.submitTopology("appActivationEvent_Topology", conf,builder.createTopology());
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
	}
	
}
