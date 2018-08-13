package com.imooc.stormTest.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.imooc.stormTest.bolt.ExclamationBolt;
import com.imooc.stormTest.bolt.PrintBolt;
import com.imooc.stormTest.spout.TestWordSpout;

/**
 * Hello world!
 * pom 1.2.2 的包不行，缺少 LocalCluster.java 是.clj文件
 * TopologyBuilder是构建拓扑的类,然后调用setSpout方法设置Spout，接着调用setBolt方法设置Bolt，
 * 最后调用createTopology方法返回StormTopology对象给submitTopology方法作为输入参数
 * 总并行度=2+2+6=10  一共2个worker  每一个worker启动10/2=5个线程
 */
public class WordsTopology {
	
	private static final Logger logger = LoggerFactory.getLogger(WordsTopology.class);
	public static void main(String[] args) {
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		topologyBuilder.setSpout("word", new TestWordSpout(), 2);//parallelism_hint 设置1个并发度
		topologyBuilder.setBolt("exclaim", new ExclamationBolt(), 2).setNumTasks(4).shuffleGrouping("word"); //设置这个bolt的task总数为4,这里配置了2个executor和4个task，所以这里每个executor执行2个task
		topologyBuilder.setBolt("print",new PrintBolt(),6).shuffleGrouping("exclaim");
		Config config = new Config();
		config.setDebug(true);
		if(args != null && args.length > 0){
			config.setNumWorkers(2);//JVM 进程数目
			try {
				StormSubmitter.submitTopologyWithProgressBar(args[0], config, topologyBuilder.createTopology());
			} catch (AlreadyAliveException e) {
				logger.error("AlreadyAliveException ={}", e);
			} catch (InvalidTopologyException e) {
				logger.error("InvalidTopologyException ={}", e);
			} catch (AuthorizationException e) {
				logger.error("AuthorizationException ={}", e);
			}
		}else{
			LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("test",config,topologyBuilder.createTopology());
            Utils.sleep(30000);
            localCluster.killTopology("test");
            localCluster.shutdown();
		}
	}
}
