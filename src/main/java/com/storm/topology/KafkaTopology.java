package com.storm.topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.log4j.Logger;
import org.apache.storm.hive.bolt.HiveBolt;
import org.apache.storm.hive.bolt.mapper.DelimitedRecordHiveMapper;
import org.apache.storm.hive.common.HiveOptions;
import org.apache.storm.kafka.*;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class KafkaTopology {
	private static final Logger logger = Logger.getLogger(KafkaTopology.class);

	public static void main(String[] args) throws Exception {

		// getting properties from topology.properties
		Properties prop = new Properties();
		InputStream input = null;
		input = new FileInputStream(args[0]);
		prop.load(input);
		String zkhost = prop.getProperty("zkhost");
		String inputTopic = prop.getProperty("inputTopic");
		String consumerGroup = prop.getProperty("consumerGroup");
		String metaStoreURI = prop.getProperty("metaStoreURI");
		String dbName = prop.getProperty("dbName");
		String tblName = prop.getProperty("tblName");


		ZkHosts zkHosts = new ZkHosts(zkhost);		
		SpoutConfig spoutConfig = new SpoutConfig(zkHosts, inputTopic, "/" + inputTopic,
				consumerGroup); 
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
		String[] colNames = {"application_id","term_months","first_name","last_name","address","state","phone","type",
				"origination_date","loan_decision_type","decided","loan_approved","denial_reason","loan_account_status",
				"approved","payment_method","requested_amount","funded","funded_date","rate","ecoagroup","officer_id","denied_date"};
		
		DelimitedRecordHiveMapper mapper = new DelimitedRecordHiveMapper().withColumnFields(new Fields(colNames));

		HiveOptions hiveOptions = new HiveOptions(metaStoreURI, dbName, tblName, mapper).withTxnsPerBatch(2).withBatchSize(10)
				.withIdleTimeout(10).withCallTimeout(10000000);

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("LoanApplicationData", kafkaSpout);
		builder.setBolt("LoanApplicationBolt", new HiveOutputBolt()).shuffleGrouping("LoanApplicationData");
		builder.setBolt("LoanApplicationOutputBolt", new HiveBolt(hiveOptions)).shuffleGrouping("LoanApplicationBolt");

		Config conf = new Config();
		conf.setNumWorkers(2);
		
		StormSubmitter.submitTopology(args[1], conf, builder.createTopology());

	}
}
