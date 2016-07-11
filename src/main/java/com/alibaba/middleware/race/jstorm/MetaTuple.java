package com.alibaba.middleware.race.jstorm;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;

class MyMsg  implements Serializable {	
	private static final long serialVersionUID = 1L;
	//private static final long serialVersionUID = 2277714452693486955L;
	private byte[] body;
	public byte[] getBody() {
		return body;
	}
	public void setBody(byte[] body) {
		this.body = body;
	}
	public String getTopic() {
		return topic;
	}
	public void setTopic(String topic) {
		this.topic = topic;
	}
	private String topic;
	public MyMsg() {
		//body = new byte[1024];
		//topic = "";
	}
	public MyMsg(byte[] data, String topic) {
		this.body = data;
		this.topic = topic;
	}
}
public class MetaTuple implements Serializable {

	/**  */
	private static final long serialVersionUID = 2277714452693486955L;

	protected ArrayList<MyMsg> msgList;	

	public void setMsgList(ArrayList<MyMsg> msgList) {
		this.msgList = msgList;
	}
	
	
	
	public MetaTuple() {
		this.msgList = new ArrayList<MyMsg>();		
	}

	public MetaTuple(List<MessageExt> msgList) {
		this.msgList = new ArrayList<MyMsg>();
		for(MessageExt msg : msgList) {
			this.msgList.add(new MyMsg(msg.getBody(), msg.getTopic()));
		}				
	}	
	
	public List<MyMsg> getMsgList() {
		return msgList;
	}
	
	public void addMyMsg(byte[] data, String topic) {
		this.msgList.add(new MyMsg(data, topic));
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this,
				ToStringStyle.SHORT_PREFIX_STYLE);
	}
}
