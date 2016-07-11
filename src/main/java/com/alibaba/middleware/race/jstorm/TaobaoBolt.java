package com.alibaba.middleware.race.jstorm;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class TaobaoBolt  implements IRichBolt {
	private final int MsgAmount = 100;
	
	protected OutputCollector collector;
	private static final Logger LOG = LoggerFactory.getLogger(TaobaoBolt.class);
	private static final long serialVersionUID = 2739902749904632871L;
	private int taskId;
	private boolean msgEnd = false;
	
	
	private HashMap<Long, Double> taobaoTradePerMin;
	private HashSet<String> payMsgAlreadyRx = new HashSet<String>();
	// 保存整分时间戳->写入tair的String key
	private HashMap<Long, String> timeToKey = new HashMap<Long, String>(100);
	// 新到达的支付消息的整分时间戳；每次计算写Tair只会写更新的部分
	private HashSet<Long> newPayTimePerMin = new HashSet<Long>();
	
	private TairOperatorImpl tairOperator;
	
	private ExecutorService execService;
	private long sleepTimeMillis;
//	private Lock lock = new ReentrantLock();
	
	private void writeToTair() {
		for(Long tbTradeKey : taobaoTradePerMin.keySet()) {
			String writeKey = null;
			Double write_val = taobaoTradePerMin.get(tbTradeKey);
			if(timeToKey.containsKey(tbTradeKey)) {
				writeKey = timeToKey.get(tbTradeKey);
			} else{
				StringBuilder strBuilder = new StringBuilder();
				strBuilder.append(RaceConfig.prex_taobao)
						  .append(RaceConfig.team_code )
						  .append("_")
						  .append(tbTradeKey);
				writeKey = strBuilder.toString();
				timeToKey.put(tbTradeKey, writeKey);
			}
			boolean ret = false;
			while(ret == false) {
				ret = tairOperator.write(writeKey, write_val);
//				LOG.info("TaobaoBolt writeKey "  + writeKey + " " + write_val + " ret:"+ret
//						+" taskId:" + taskId);
			}
			
		}
	}
	
	@Override
	public void cleanup() {
		
	}

	@Override
	public void execute(Tuple tuple) {
		String streamId = tuple.getSourceStreamId();
		if(streamId.equals(RaceConfig.TB_STREAM_ID)) {
			double payAmount = (double) tuple.getDoubleByField("payamount");
			long timePerMin = tuple.getLongByField("time");
			long createTime = tuple.getLongByField("createTime");
			long orderId = tuple.getLongByField("orderid");
//			LOG.info("TaobaoBolt got paymsg taskId:" + taskId + " orderId"  + orderId + " timePerMin:" + timePerMin + " payAmount:"+payAmount);
			StringBuilder strBuilder = new StringBuilder();
			strBuilder.append(payAmount)
			.append(orderId)
			.append(createTime);
			String key = strBuilder.toString();
			if(!payMsgAlreadyRx.contains(key)) {
				payMsgAlreadyRx.add(key);
				double oldVal = 0;
				if(taobaoTradePerMin.containsKey(timePerMin)) {
					oldVal = taobaoTradePerMin.get(timePerMin);
				}
				taobaoTradePerMin.put(timePerMin, payAmount + oldVal);
//				newPayTimePerMin.add(timePerMin);
			}
		} else if(streamId.equals(RaceConfig.STOP_STREAM_ID)) {
			LOG.info("TaobaoBolt  got stop msg taskId:" + taskId);
			msgEnd = true;
		}
		if(msgEnd) {
//			writeToTair();
			execService.submit(new Runnable() {
				@Override
				public void run() {
					writeToTair();
				}
			});
		}
		collector.ack(tuple);
	}

	@Override
	public void prepare(Map arg0, TopologyContext context, OutputCollector output) {
		tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
                RaceConfig.TairGroup, RaceConfig.TairNamespace);
		tairOperator.initTair();
		taobaoTradePerMin = new HashMap<Long, Double>(MsgAmount/2);
		
		taskId = context.getThisTaskId();
			
		LOG.info("TaobaoBolt  prepare "+ " taskId:" + taskId);
		
		this.collector = output;
		
		// Sleep 17 minute
		sleepTimeMillis = 17 * 60 * 1000;
		
		execService = Executors.newFixedThreadPool(1);
		new Thread(new Runnable() {
				@Override
				public void run() {
					while(!msgEnd) {
						try{
							Thread.sleep(sleepTimeMillis);
						}catch (InterruptedException e) {
							e.printStackTrace();
						}
						writeToTair();
					}
				}
			}).start();;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
//		declarer.declare(new Fields("TaobaoBolt"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}