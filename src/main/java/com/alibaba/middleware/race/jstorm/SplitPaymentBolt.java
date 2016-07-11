package com.alibaba.middleware.race.jstorm;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.rocketmq.common.message.MessageExt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

class SplitMsg{
	private String topic;
	private long orderId;
	private double payAmount;
	private long createTime;
	private short payPlatform;
	private short paySource;
	
	public SplitMsg(String topic, long orderId, double payAmount,
			long time, short platform, short source) {
		this.topic = topic;
		this.orderId = orderId;
		this.payAmount = payAmount;
		this.createTime = time;
		this.payPlatform = platform;
		this.paySource = source;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public long getOrderId() {
		return orderId;
	}

	public void setOrderId(long orderId) {
		this.orderId = orderId;
	}

	public double getPayAmount() {
		return payAmount;
	}

	public void setPayAmount(double payAmount) {
		this.payAmount = payAmount;
	}

	public long getCreateTime() {
		return createTime;
	}

	public void setCreateTime(long createTime) {
		this.createTime = createTime;
	}

	public short getPayPlatform() {
		return payPlatform;
	}

	public void setPayPlatform(short payPlatform) {
		this.payPlatform = payPlatform;
	}

	public short getPaySource() {
		return paySource;
	}

	public void setPaySource(short paySource) {
		this.paySource = paySource;
	}
	
	
}
public class SplitPaymentBolt implements IRichBolt{
	
	private final int PAYMSG_AMOUNT = 100; 
	private final int BOLT_NUM = 16;
	
	protected OutputCollector collector;
	private static final Logger LOG = LoggerFactory.getLogger(SplitPaymentBolt.class);
	private static final long serialVersionUID = -1134480330621788851L;
	
	
	private int taskId;
	// TODO: we can store these two order id to Tair to save memory 
	private HashSet<Long> taobaoOrderId = new HashSet<Long>(PAYMSG_AMOUNT);
	private HashSet<Long> tmallOrderId = new HashSet<Long>(PAYMSG_AMOUNT);
	// Save the payment msg which not emitted. Value=paymentMsg key
	private HashMap<Long, List<String>>    payMsgCache = new HashMap<Long, List<String>>(PAYMSG_AMOUNT / 2);
	private HashMap<String, Double> payMsgAmount = new HashMap<String, Double>(PAYMSG_AMOUNT);
	private HashMap<String, Long> payMsgCreateTimePerMin = new HashMap<String, Long>(PAYMSG_AMOUNT);
	private HashMap<String, Long> payMsgCreateTime = new HashMap<String, Long>();
	private HashSet<String> payMsgAlreadyRx = new HashSet<String>(PAYMSG_AMOUNT / 2);
	
	private  LinkedBlockingDeque<SplitMsg> queue = new LinkedBlockingDeque<SplitMsg>();
	
	@Override
	public void cleanup() {
		
	}
	
	private void handleMsg() {
		SplitMsg msg = null;
		try {
			msg = this.queue.take();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if(msg == null) {
			return;
		}
		String topic = msg.getTopic();
		int topicVal = -1;
		if(topic.equals(RaceConfig.MqTaobaoTradeTopic)) {
			topicVal = 1;
		} else if(topic.equals(RaceConfig.MqTmallTradeTopic)) {
			topicVal = 2;
		}else if(topic.equals(RaceConfig.MqPayTopic)) {
			topicVal = 3;
		}
		if(topicVal == 1 || topicVal == 2) {
			long orderId = msg.getOrderId();
			if(topicVal == 1) {
				taobaoOrderId.add(orderId);
			}else if(topicVal == 2) {
				tmallOrderId.add(orderId);
			}
			if(payMsgCache.containsKey(orderId)) {
				// 有缓存的payment没有发送
				List<String> payMsgKeyList = payMsgCache.get(orderId);
				if(payMsgKeyList.size() > 0) {
					for(String key : payMsgKeyList) {
						double payAmount = payMsgAmount.get(key);
						long createTimePerMin = payMsgCreateTimePerMin.get(key);
						payMsgAmount.remove(key);
						payMsgCreateTimePerMin.remove(key);
						long hashVal = ((createTimePerMin / 60) % BOLT_NUM);
						long createTime = payMsgCreateTime.get(key);
						if(topicVal == 1) {
							collector.emit(RaceConfig.TB_STREAM_ID, new Values(payAmount, createTimePerMin, orderId, hashVal, createTime));
						}else if(topicVal == 2) {
							collector.emit(RaceConfig.TM_STREAM_ID, new Values(payAmount, createTimePerMin, orderId, hashVal, createTime));
						}
					}
					payMsgKeyList.clear();
				}
			}
		}else if(topicVal == 3) {
				long orderId = msg.getOrderId();
				double payAmount = msg.getPayAmount();
				if (orderId == 0 && payAmount < 0) {
//					LOG.info("SplitPaymentBolt get payment end msg" + " taskId:" +taskId);
					collector.emit(RaceConfig.STOP_STREAM_ID, new Values(-1));
					return;
				}								
				long createTime = msg.getCreateTime();
				long createTimePerMin = RaceUtils.getTime(createTime);
				short payPlatform = msg.getPayPlatform();
				short paySource = msg.getPaySource();
				
				StringBuilder strBuilder = new StringBuilder();
				strBuilder.append(orderId)
						  .append(createTime)
						  .append(payAmount)
						  .append(payPlatform)
						  .append(paySource);
				String payMsgKey = strBuilder.toString();
				if(payMsgAlreadyRx.contains(payMsgKey)) {
					return;
				} else{
					payMsgAlreadyRx.add(payMsgKey);
				}
				long hashVal = ((createTimePerMin / 60) % BOLT_NUM);
//				LOG.info("SplitPaymentBolt paymentMsg orderId:" + orderId +" payPlatform:" + payPlatform + " createTime:" + createTime
//						+ " createTimePerMin:"+createTimePerMin + " taskId:" +taskId);
				if(taobaoOrderId.contains(orderId)) {
//					LOG.info("SplitPaymentBolt taskId:" +taskId + " emit taobao pay msg");
					collector.emit(RaceConfig.TB_STREAM_ID, new Values(payAmount, createTimePerMin, 
							orderId, hashVal, createTime));
				} else if(tmallOrderId.contains(orderId)) {
//					LOG.info("SplitPaymentBolt taskId:" +taskId + " emit tmall pay msg");
					collector.emit(RaceConfig.TM_STREAM_ID, new Values(payAmount, createTimePerMin, 
							orderId, hashVal, createTime));
				} else{
					// 暂时存着
					if(payMsgCache.containsKey(orderId)) {
						// 之前已经有这个OrderID的支付信息缓存着，则添加
						List<String> payMsgKeyList = payMsgCache.get(orderId);
						payMsgKeyList.add(payMsgKey);
						
					} else{
						List<String> payMsgKeyList = new ArrayList<String>();
						payMsgKeyList.add(payMsgKey);
						payMsgCache.put(orderId, payMsgKeyList);
					}
					payMsgAmount.put(payMsgKey, payAmount);
					payMsgCreateTimePerMin.put(payMsgKey, createTimePerMin);
					payMsgCreateTime.put(payMsgKey, createTime);
				}
				collector.emit(RaceConfig.PAY_STREAM_ID, new Values(payAmount, 
						createTimePerMin, payPlatform, orderId, hashVal, createTime));
			}
	}
	
	@Override
	public void execute(Tuple tuple) {
		String topic = tuple.getStringByField("topic");
		long orderId = tuple.getLongByField("orderid");
		double payAmount = tuple.getDoubleByField("payamount");
		long createTime = tuple.getLongByField("time");
		short payPlatform = tuple.getShortByField("payPlatform");
		short paySource = tuple.getShortByField("paySource");
		try {
			this.queue.put(new SplitMsg(topic, orderId, payAmount, createTime, 
					payPlatform, paySource));
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		collector.ack(tuple);
	}

	@Override
	public void prepare(Map arg0, TopologyContext context, OutputCollector output) {
		this.collector = output;
		this.taskId = context.getThisTaskId();
		LOG.info("SplitPaymentBolt prepare taskId:" + taskId);
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
		declarer.declareStream(RaceConfig.TB_STREAM_ID, new Fields("payamount", "time", "orderid", "hash", "createTime"));
		declarer.declareStream(RaceConfig.TM_STREAM_ID, new Fields("payamount", "time", "orderid", "hash", "createTime"));
		declarer.declareStream(RaceConfig.PAY_STREAM_ID, new Fields("payamount", "time", "platform", "orderid", "hash", "createTime"));
		declarer.declareStream(RaceConfig.STOP_STREAM_ID, new Fields("stop"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}