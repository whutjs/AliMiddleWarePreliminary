package com.alibaba.middleware.race.jstorm;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.model.PaymentMessage;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

class RatioMsg{
	private double payAmount;
	private long createTime;
	private long timePerMin;
	private short platform;
	private long orderId;
	
	public RatioMsg(double pay, long createTime, long timePerMin,
			short platform, long orderId) {
		this.payAmount = pay;
		this.createTime = createTime;
		this.timePerMin = timePerMin;
		this.platform = platform;
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

	public long getTimePerMin() {
		return timePerMin;
	}

	public void setTimePerMin(long timePerMin) {
		this.timePerMin = timePerMin;
	}

	public short getPlatform() {
		return platform;
	}

	public void setPlatform(short platform) {
		this.platform = platform;
	}

	public long getOrderId() {
		return orderId;
	}

	public void setOrderId(long orderId) {
		this.orderId = orderId;
	}
	
}
public class RatioBolt   implements IRichBolt {
	private static final Logger LOG = LoggerFactory.getLogger(RatioBolt.class);
	private static final long serialVersionUID = -4615617335741798212L;
	private TreeMap<Long, Double> pcTradeMap = new TreeMap<>();
	private TreeMap<Long, Double> mobileTradeMap = new TreeMap<>();
	private HashSet<String> recvPaymentMsg = new HashSet<String>();
	private boolean payMsgEnd = false;
	private int taskId;
	private TairOperatorImpl tairOperator;
	protected OutputCollector collector;
	
	private ExecutorService execService;
	private long sleepTimeMillis;
//	private Lock lock = new ReentrantLock();
	
	private  LinkedBlockingDeque<RatioMsg> queue = new LinkedBlockingDeque<RatioMsg>();
	private  LinkedBlockingDeque<Short> signalQueue = new LinkedBlockingDeque<Short>();
	
	// 保存整分时间戳->写入tair的String key
	private HashMap<Long, String> timeToKey = new HashMap<Long, String>(100);
		
	@Override
	public void cleanup() {
		
	}
	
	private void calculateAndWrite(){
		double currentMobileTradeAmount= 0.0;
		double currentPcTradeAmount= 0.0;
		long offset = 60L; //per min 
		

		Iterator<Long> it1 = mobileTradeMap.keySet().iterator();
		Iterator<Long> it2 = pcTradeMap.keySet().iterator();
		if(!it1.hasNext() || !it2.hasNext()) {
			return;
		}
		
		it1 = mobileTradeMap.keySet().iterator();
		it2 = pcTradeMap.keySet().iterator();
		Long firstkey1 = it1.next();
		Long firstkey2 = it2.next();
		long currenttime = (firstkey1 <= firstkey2 )?firstkey1: firstkey2;
		currentMobileTradeAmount = 1;
		if( mobileTradeMap.containsKey(currenttime)) {
			currentMobileTradeAmount = mobileTradeMap.get(firstkey1).doubleValue();
		}
		currentPcTradeAmount = 1.0; 
		if(pcTradeMap.containsKey(currenttime)) {
			currentPcTradeAmount = pcTradeMap.get(currenttime).doubleValue();
		}
		 offset = 60L; //per min 
		if(firstkey1 > firstkey2) {
			// 大的要重置
			it1 = mobileTradeMap.keySet().iterator();
		}
		if(firstkey1 < firstkey2) {
			it2 = pcTradeMap.keySet().iterator();
		}
		while(true){
			String writeKey = null;
			double ratio = currentMobileTradeAmount / currentPcTradeAmount;
			Double write_val = ratio;
			if(timeToKey.containsKey(currenttime)) {
				writeKey = timeToKey.get(currenttime);
			}else{
				writeKey = RaceConfig.prex_ratio + RaceConfig.team_code + "_" +  currenttime;
				timeToKey.put(currenttime, writeKey);
			}
			boolean ret = false;
			while(ret == false) {
				ret = tairOperator.write(writeKey, write_val);
//				LOG.info("RatioBolt writeKey " + writeKey + " " + write_val 
//						+ " ret:" + ret + " taskId:" + taskId);
				
			}
			
			currenttime += offset;
			if(!it1.hasNext() && !it2.hasNext())
				break;
			if( mobileTradeMap.containsKey(currenttime)) {
				currentMobileTradeAmount += mobileTradeMap.get(currenttime).doubleValue();
				Long  val = it1.next();
			}
			if(pcTradeMap.containsKey(currenttime)) {
				currentPcTradeAmount += pcTradeMap.get(currenttime).doubleValue();
				Long  val = it2.next();
			}			
			
			
		}
		
		
	}
	
	private void calAndWriteThread() {
		// 当队列信号处理完毕，说明当前可能没有消息了
		Short signal = null;
		while(signal == null) {
			signal = signalQueue.poll();
		}
		if(payMsgEnd) {
			calculateAndWrite();
		}
	}
	private void handleMsgThread() {
		RatioMsg msg = null;
		try {
			msg = queue.take();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if(msg == null) {
			return;
		}
		double payAmount = msg.getPayAmount();
		long createTime = msg.getCreateTime();
		long timePerMin = msg.getTimePerMin();
		short platform = msg.getPlatform();
		long orderId = msg.getOrderId();
		StringBuilder strBuilder = new StringBuilder();
		strBuilder.append(createTime)
				.append(payAmount)
				.append(platform)
				.append(orderId);
		String unique_key = strBuilder.toString();
		boolean flag = recvPaymentMsg.contains(unique_key);
		if(flag == false) {
			TreeMap<Long, Double> pltfmTradeMap = null;
			if(platform == 0) {
				// PC
				pltfmTradeMap = pcTradeMap;
			}else{
				// mobile
				pltfmTradeMap = mobileTradeMap;
			}
			if(pltfmTradeMap.containsKey(timePerMin)){
				Double old = pltfmTradeMap.get(timePerMin);
				old += payAmount;
				pltfmTradeMap.put(timePerMin, old);
			} else {					
				//insert
				pltfmTradeMap.put(timePerMin, payAmount);
			}
			recvPaymentMsg.add(unique_key);
			if(payMsgEnd && queue.size() < 5) {
				calculateAndWrite();
			}
//			gotNewMsg = true;
//			try {
//				signalQueue.put((short) 1);
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
		}
	}

	@Override
	public void execute(Tuple tuple) {
			String streamId = tuple.getSourceStreamId();
			boolean gotNewMsg = false;
			if(streamId.equals(RaceConfig.PAY_STREAM_ID)) {
				double payAmount = (double) tuple.getDoubleByField("payamount");
				long createTime = tuple.getLongByField("createTime");
				long timePerMin = tuple.getLongByField("time");
				short platform = tuple.getShortByField("platform");
				long orderId = tuple.getLongByField("orderid");
//				LOG.info("RatioBolt got paymsg taskId:" + taskId + " orderId:" + orderId + 
//							" platform:" + platform + " payAmount:" +payAmount + " timePerMin:" + timePerMin);
				if(platform != 0 && platform != 1) {
					collector.ack(tuple);
					return;
				}
				queue.add(new RatioMsg(payAmount, createTime, timePerMin, platform, orderId));
				
			}else if(streamId.equals(RaceConfig.STOP_STREAM_ID)) {
//				LOG.info("RatioBolt got stop msg taskId:" + taskId);
				payMsgEnd = true;
				gotNewMsg = true;
			}
			
			collector.ack(tuple);
			
			
	}

	@Override
	public void prepare(Map arg0, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		taskId = context.getThisTaskId();
		LOG.info("RatioBolt prepare taskId:" + taskId);
		
		tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
                RaceConfig.TairGroup, RaceConfig.TairNamespace);
		tairOperator.initTair();
		
		
		// Sleep 17 minute
		sleepTimeMillis = 17 * 60 * 1000;
		execService = Executors.newFixedThreadPool(1);
		execService.submit(new Runnable() {
				@Override
				public void run() {					
					while(!payMsgEnd) {
						try{
							Thread.sleep(sleepTimeMillis);
						}catch (InterruptedException e) {
							e.printStackTrace();
						}
						calculateAndWrite();
					}
				}
			});
		
		new Thread(new Runnable() {
			@Override
			public void run() {					
				while(true) {
					handleMsgThread();
				}
			}
		}).start();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
//		declarer.declare(new Fields("RatioBolt"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}