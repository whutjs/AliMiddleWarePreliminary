package com.alibaba.middleware.race.jstorm;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.rocketmq.client.MQHelper;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;


public class MetaConsumerFactory {
	
	private static final Logger	LOG   = Logger.getLogger(MetaConsumerFactory.class);
	
    
    private static final long   serialVersionUID = 4641537253577312163L;
    
    public static Map<String, DefaultMQPushConsumer> consumers = 
    		new HashMap<String, DefaultMQPushConsumer>();
    
    public static synchronized DefaultMQPushConsumer mkInstance(Map config, 
			MessageListenerConcurrently listener)  throws Exception{
    	
    	String topic = RaceConfig.MqTaobaoTradeTopic + RaceConfig.MqTmallTradeTopic
    			+ RaceConfig.MqPayTopic;
    	String groupId = RaceConfig.MetaConsumerGroup;
    	
    	String key = topic + "@" + groupId;
    	
    	DefaultMQPushConsumer consumer = consumers.get(key);
    	if (consumer != null) {
    		
    		LOG.info("Consumer of " + key + " has been created, don't recreate it ");
    		
    		//Attention, this place return null to info duplicated consumer
    		return null;
    	}
    	
        
        StringBuilder sb = new StringBuilder();
        sb.append("Begin to init meta client \n");
        sb.append(",configuration:").append(config);
        
        LOG.info(sb.toString());
        
        consumer = new DefaultMQPushConsumer(groupId);
//        // 本地跑需注释去掉
//        String nameServer = "192.168.122.66:9876";
//        if ( nameServer != null) {
//			String namekey = "rocketmq.namesrv.domain";
//
//			String value = System.getProperty(namekey);
//			// this is for alipay
//			if (value == null) {
//				System.setProperty(namekey, nameServer);
//			} else if (value.equals(nameServer) == false) {
//				throw new Exception(
//						"Different nameserver address in the same worker "
//								+ value + ":" + nameServer);
//
//			}
//		}
//        consumer.setNamesrvAddr(nameServer);
//        // 本地跑需注释去掉
        
        String instanceName = groupId +"@" +	JStormUtils.process_pid();
       
		consumer.setInstanceName(instanceName);
		consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
		consumer.subscribe(RaceConfig.MqTaobaoTradeTopic, "*");
		consumer.subscribe(RaceConfig.MqTmallTradeTopic, "*");
		consumer.subscribe(RaceConfig.MqPayTopic, "*");
		consumer.registerMessageListener(listener);
		
		consumer.setPullThresholdForQueue(256);
		consumer.setConsumeMessageBatchMaxSize(128);
		//consumer.setPullBatchSize(32);
		//consumer.setPullInterval(0);
		//consumer.setConsumeThreadMin(4);
		//consumer.setConsumeThreadMax(4);

		consumer.start();
		
		consumers.put(key, consumer);
		LOG.info("Successfully create " + key + " consumer");
		
		
		return consumer;
		
    }
    
}
