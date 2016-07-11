package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Count implements IRichBolt {
	private static final long serialVersionUID = 2495121976857546346L;
	private static final Logger LOG = LoggerFactory.getLogger(Count.class);
	protected OutputCollector collector;
	
	private boolean payTopicEnd = false;
	private boolean taobaoTopicEnd = false;
	private boolean tmallTopicEnd = false;
	private TairOperatorImpl tairOperator;
//	private boolean recvSignal = false;
	private int taskId;
	//private ExecutorService writeTairThread;
	// Store payment msgs that have been received
	private HashSet<String> recvPaymentMsg = new HashSet<String>();
	 
	// For time record
	private long prepareTime;
	
	private int cnt = 0;
	

	@Override
	public void execute(Tuple tuple) {
//		int msg = (int) tuple.getValue(0);
//		if(msg < 0) {
//			LOG.info("task:"+ taskId +" cnt:" + cnt);
//		} else{
//			LOG.info("task: " + taskId + " msg:" + msg);
//		}
		
		collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("count"));
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		taskId = context.getThisTaskId();
//		LOG.info("Current Count task ID:" + taskId + " prepare");
//		tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
//                RaceConfig.TairGroup, RaceConfig.TairNamespace);
//		tairOperator.initTair();
		
//		writeTairThread = Executors.newFixedThreadPool(1);
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}
