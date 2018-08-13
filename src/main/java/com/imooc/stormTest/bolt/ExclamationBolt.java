package com.imooc.stormTest.bolt;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExclamationBolt extends BaseRichBolt{

	private static final Logger logger = LoggerFactory.getLogger(ExclamationBolt.class);
	
	private OutputCollector collector = null;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		try {
			this.collector.emit(input, new Values(input.getString(0)+"!!!"));
		} catch (Exception e) {
			logger.info("Bolt 发送数据失败： collector exception ={}", e);
		}
		try {
			this.collector.ack(input);
		} catch (Exception e) {
			logger.info(" Exclamation Bolt Ack 确认数据失败： collector exception ={}", e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}
