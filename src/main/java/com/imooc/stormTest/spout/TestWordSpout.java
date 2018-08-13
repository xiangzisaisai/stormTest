package com.imooc.stormTest.spout;

import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author wing
 * @date 2018年8月13日 上午11:37:36 
 * @Description: 只能保证TestWordSpout->ExclamationBolt的消息传递的可靠性
 */
public class TestWordSpout extends BaseRichSpout{
	
	private static final Logger logger = LoggerFactory.getLogger(TestWordSpout.class);
	private SpoutOutputCollector collector = null;
	private ConcurrentHashMap<UUID, Values> pending = null;
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		Utils.sleep(1000);
		final String[] words = new String[]{"fdfs","fdfs","ffsdfs"};
		final Random random = new Random();
		final String word = words[random.nextInt(words.length)];
		Values values = new Values(word);
		try {
			UUID msgId = UUID.randomUUID();
			this.collector.emit(values, msgId);
			this.pending.put(msgId, values);
			logger.info("Word Spout={}, msgId ={}",values, msgId);
		} catch (Exception e) {
			logger.info("Spout 发送数据失败： collector exception ={}", e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}
	
	@Override
	public void ack(Object msgId) {
		logger.info("#####[ack]###### msgId={}, values ={}",msgId, this.pending.get(msgId));
		this.pending.remove(msgId);
	}

	@Override
	public void fail(Object msgId) {
		logger.info("#####[fail]###### msgId={}, values ={}",msgId, this.pending.get(msgId));
		this.collector.emit(this.pending.get(msgId), msgId);
	}
}
