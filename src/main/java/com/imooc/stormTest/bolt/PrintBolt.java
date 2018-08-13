package com.imooc.stormTest.bolt;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrintBolt extends BaseRichBolt{
	
	private static Logger logger = LoggerFactory.getLogger(PrintBolt.class);

	private OutputCollector collector = null;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		logger.info("input ={}", input.getString(0) + "......");
		try {
			this.collector.ack(input);
		} catch (Exception e) {
			logger.info(" Print Bolt Ack 确认数据失败： collector exception ={}", e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}
