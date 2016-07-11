
package com.alibaba.middleware.race.rocketmq;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendCallback;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.middleware.race.model.*;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.jstorm.Count;
import com.alibaba.middleware.race.jstorm.MetaSpout;
import com.alibaba.middleware.race.jstorm.MetaTuple;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;




/**
 * Producer，发送消息
 */
public class Producer {
	private static final Logger LOG = Logger.getLogger(Count.class);
    private static Random rand = new Random();
    private static int count = 300;
    
    public static MetaTuple produceMsg() throws InterruptedException {
    	 
    	 final String [] topics = new String[]{RaceConfig.MqTaobaoTradeTopic, RaceConfig.MqTmallTradeTopic};
         //final Semaphore semaphore = new Semaphore(0);
    	 MetaTuple msgToBroker = new MetaTuple();
    	 LOG.info("produce msg");
         for (int i = 0; i < count; i++) {
             try {
                  int platform = rand.nextInt(2);
                  OrderMessage orderMessage = ( platform == 0 ? OrderMessage.createTbaoMessage() : OrderMessage.createTmallMessage());
                 orderMessage.setCreateTime(System.currentTimeMillis());

                 byte [] body = RaceUtils.writeKryoObject(orderMessage);
                 
                 msgToBroker.addMyMsg(body, topics[platform]);
                 LOG.info(new Date().toString()+"---"+orderMessage);
                

                 //Send Pay message
                 PaymentMessage[] paymentMessages = PaymentMessage.createPayMentMsg(orderMessage);
                 double amount = 0;
                 for (PaymentMessage paymentMessage : paymentMessages) {
                     int retVal = Double.compare(paymentMessage.getPayAmount(), 0);
                     if (retVal < 0) {
                         throw new RuntimeException("price < 0 !!!!!!!!");
                     }

                     if (retVal > 0) {
                         amount += paymentMessage.getPayAmount();
                         msgToBroker.addMyMsg(RaceUtils.writeKryoObject(paymentMessage), RaceConfig.MqPayTopic);
                         LOG.info(new Date().toString()+"---"+paymentMessage);
                     }else {
                         //
                     }
                 }

                 if (Double.compare(amount, orderMessage.getTotalPrice()) != 0) {
                     throw new RuntimeException("totalprice is not equal.");
                 }


             } catch (Exception e) {
                 e.printStackTrace();
                 Thread.sleep(1000);
             }
         }
         //用一个short标识生产者停止生产数据
         byte [] zero = new  byte[]{0,0};
         msgToBroker.addMyMsg(zero, RaceConfig.MqTaobaoTradeTopic);
         msgToBroker.addMyMsg(zero, RaceConfig.MqTmallTradeTopic);
         msgToBroker.addMyMsg(zero, RaceConfig.MqPayTopic);

         return msgToBroker;
    }
    /**
     * 这是一个模拟堆积消息的程序，生成的消息模型和我们比赛的消息模型是一样的，
     * 所以选手可以利用这个程序生成数据，做线下的测试。
     * @param args
     * @throws MQClientException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws MQClientException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");

        //在本地搭建好broker后,记得指定nameServer的地址
        producer.setNamesrvAddr("192.168.122.66:9876");

        producer.start();

        final String [] topics = new String[]{RaceConfig.MqTaobaoTradeTopic, RaceConfig.MqTmallTradeTopic};
        final Semaphore semaphore = new Semaphore(0);

        for (int i = 0; i < count; i++) {
            try {
                final int platform = rand.nextInt(2);
                final OrderMessage orderMessage = ( platform == 0 ? OrderMessage.createTbaoMessage() : OrderMessage.createTmallMessage());
                orderMessage.setCreateTime(System.currentTimeMillis());

                byte [] body = RaceUtils.writeKryoObject(orderMessage);

                Message msgToBroker = new Message(topics[platform], body);

                producer.send(msgToBroker, new SendCallback() {
                    public void onSuccess(SendResult sendResult) {
                        System.out.println(orderMessage);
                        semaphore.release();
                    }
                    public void onException(Throwable throwable) {
                        throwable.printStackTrace();
                    }
                });

                //Send Pay message
                PaymentMessage[] paymentMessages = PaymentMessage.createPayMentMsg(orderMessage);
                double amount = 0;
                for (final PaymentMessage paymentMessage : paymentMessages) {
                    int retVal = Double.compare(paymentMessage.getPayAmount(), 0);
                    if (retVal < 0) {
                        throw new RuntimeException("price < 0 !!!!!!!!");
                    }

                    if (retVal > 0) {
                        amount += paymentMessage.getPayAmount();
                        final Message messageToBroker = new Message(RaceConfig.MqPayTopic, RaceUtils.writeKryoObject(paymentMessage));
                        producer.send(messageToBroker, new SendCallback() {
                            public void onSuccess(SendResult sendResult) {
                                System.out.println(paymentMessage);
                            }
                            public void onException(Throwable throwable) {
                                throwable.printStackTrace();
                            }
                        });
                    }else {
                        //
                    }
                }

                if (Double.compare(amount, orderMessage.getTotalPrice()) != 0) {
                    throw new RuntimeException("totalprice is not equal.");
                }


            } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(1000);
            }
        }

        semaphore.acquire(count);

        //用一个short标识生产者停止生产数据
        byte [] zero = new  byte[]{0,0};
        Message endMsgTB = new Message(RaceConfig.MqTaobaoTradeTopic, zero);
        Message endMsgTM = new Message(RaceConfig.MqTmallTradeTopic, zero);
        Message endMsgPay = new Message(RaceConfig.MqPayTopic, zero);

        try {
            producer.send(endMsgTB);
            producer.send(endMsgTM);
            producer.send(endMsgPay);
        } catch (Exception e) {
            e.printStackTrace();
        }
        producer.shutdown();
    }
}
