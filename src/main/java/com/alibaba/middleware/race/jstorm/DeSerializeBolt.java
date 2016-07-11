package com.alibaba.middleware.race.jstorm;

import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class DeSerializeBolt implements IRichBolt{
	
	protected OutputCollector collector;
	private static final Logger LOG = LoggerFactory.getLogger(DeSerializeBolt.class);
	private static final long serialVersionUID = -1982208920631406382L;
	protected LinkedBlockingDeque<MetaTuple> queue = new LinkedBlockingDeque<MetaTuple>();
	
	private int taskId;
	
	@Override
	public void cleanup() {
		
	}
	
	private void handleMsg() {
		MetaTuple metaTuple = null;
		try {
			metaTuple = queue.take();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if(metaTuple == null) {
			return;
		}
		for(MyMsg msg : metaTuple.getMsgList()) {
			String topic = msg.getTopic();
			short paySouce = 0;
			short payPlatform = 0;
			if(topic.equals(RaceConfig.MqPayTopic)) {
				byte[] body = msg.getBody();
				if (body.length == 2 && body[0] == 0 && body[1] == 0) {
					collector.emit(new Values(0L, topic, -1.0, paySouce, payPlatform, 0L));
					continue;
				}
				PaymentMessage paymentMsg = RaceUtils.readKryoObject(
						PaymentMessage.class, body);
//				LOG.info("DeSerializeBolt taskId:" +taskId + " paymentMsg " + paymentMsg);
				double payAmount = paymentMsg.getPayAmount();
				long createTime = paymentMsg.getCreateTime();
				long orderId = paymentMsg.getOrderId();
				paySouce = paymentMsg.getPaySource();
				payPlatform = paymentMsg.getPayPlatform();
				collector.emit(new Values(orderId, topic, payAmount, paySouce, payPlatform, createTime));
			}else if(topic.equals(RaceConfig.MqTaobaoTradeTopic) || 
					topic.equals(RaceConfig.MqTmallTradeTopic)){
				byte[] body = msg.getBody();
				if (body.length == 2 && body[0] == 0 && body[1] == 0) {
					continue;
				}
				OrderMessage orderMsg = RaceUtils.readKryoObject(
						OrderMessage.class, body);
//				LOG.info("DeSerializeBolt taskId:" +taskId + " orderMsg " + orderMsg);
				collector.emit(new Values(orderMsg.getOrderId(), topic, 0.0, paySouce, payPlatform, 0L));
			}
		}
	}
	@Override
	public void execute(Tuple tuple) {
		MetaTuple metaTuple = (MetaTuple) tuple.getValue(0);
		if (metaTuple == null) {
			this.collector.ack(tuple);
			return;
		}
		try {
			this.queue.put(metaTuple);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.collector.ack(tuple);
	}

	@Override
	public void prepare(Map arg0, TopologyContext context, OutputCollector output) {
		this.collector = output;
		this.taskId = context.getThisTaskId();
		new Thread(new Runnable() {
			
			@Override
			public void run() {
				while(true) {
					handleMsg();
				}
			}
		}).start();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("orderid", "topic", "payamount", "paySource", "payPlatform", "time"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
