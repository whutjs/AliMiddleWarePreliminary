package com.alibaba.middleware.race.jstorm;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.alibaba.jstorm.client.spout.IAckValueSpout;
import com.alibaba.jstorm.client.spout.IFailValueSpout;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.middleware.race.rocketmq.Producer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;


public class MetaSpout implements IRichSpout, IAckValueSpout, IFailValueSpout,
		MessageListenerConcurrently {
	
	/**  Test 
	private int data[];
	private int idx;
	private boolean signal = false;
	// For test
	private MetaTuple produceMsgs;
	**/ 
	/**  */
	private static final long serialVersionUID = 8476906628618859716L;
	private static final Logger LOG = Logger.getLogger(MetaSpout.class);
	private int taskId;
	
	protected SpoutOutputCollector collector;
	protected transient DefaultMQPushConsumer consumer;

	protected String id;
	
	private final int QUEUE_LIMIT = 300000; 
	protected transient LinkedBlockingDeque<MetaTuple> sendingQueue;
	
	// For test
//	private int idx = 0;
//	private boolean stop = false;
	
	public MetaSpout() {
		
	}

	

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		
		this.taskId = context.getThisTaskId();
		this.collector = collector;
		this.id = context.getThisComponentId() + ":" + context.getThisTaskId();
		this.taskId = context.getThisTaskId();
		this.sendingQueue = new LinkedBlockingDeque<MetaTuple>(QUEUE_LIMIT);

		
		StringBuilder sb = new StringBuilder();
		sb.append("Begin to init MetaSpout:").append(id);
		LOG.info( sb.toString());
		

		try {
			consumer = MetaConsumerFactory.mkInstance(conf, this);
		} catch (Exception e) {
			LOG.error("Failed to create Meta Consumer ", e);
			throw new RuntimeException("Failed to create MetaConsumer" + id, e);
		}

		if (consumer == null) {
			LOG.warn(id
					+ " already exist consumer in current worker, don't need to fetch data ");

			new Thread(new Runnable() {

				@Override
				public void run() {
					while (true) {
						try {
							Thread.sleep(1000000);
						} catch (InterruptedException e) {
							break;
						}

						StringBuilder sb = new StringBuilder();
						sb.append("Only on meta consumer can be run on one process,");
						sb.append(" but there are mutliple spout consumes with the same topic@groupid meta, so the second one ");
						sb.append(id).append(" do nothing ");
						LOG.info(sb.toString());
					}
				}
			}).start();
		}
		
		LOG.info("Successfully init " + id);
	}

	@Override
	public void close() {
		if (consumer != null) {
			consumer.shutdown();
		}

	}

	@Override
	public void activate() {
		if (consumer != null) {
			consumer.resume();
		}

	}

	@Override
	public void deactivate() {
		if (consumer != null) {
			consumer.suspend();
		}
	}


	@Override
	public void nextTuple() {
		MetaTuple tuple = null;
		try {
			tuple = this.sendingQueue.take();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		if(tuple == null) {
			return;
		}
		
		// TODO: add msgid 
		collector.emit(new Values(tuple));
		
	}

	@Deprecated
	public void ack(Object msgId) {
		LOG.warn("Shouldn't go this function");
	}

	@Deprecated
	public void fail(Object msgId) {
		LOG.warn("Shouldn't go this function");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("MetaSpout"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@Override
	public void fail(Object msgId, List<Object> values) {
		MetaTuple metaTuple = (MetaTuple) values.get(0);
		collector.emit(new Values(metaTuple));
	}


	@Override
	public void ack(Object msgId, List<Object> values) {
//		MetaTuple metaTuple = (MetaTuple) values.get(0);
//		finishTuple(metaTuple);
	}

	@Override
	public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
			ConsumeConcurrentlyContext context) {
		MetaTuple metaTuple = new MetaTuple(msgs);
		sendingQueue.offer(metaTuple);		
		return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;	


	}

	public DefaultMQPushConsumer getConsumer() {
		return consumer;
	}

}