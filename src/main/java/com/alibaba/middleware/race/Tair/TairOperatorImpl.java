package com.alibaba.middleware.race.Tair;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.jstorm.Count;
import com.esotericsoftware.minlog.Log;

import java.io.Serializable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.tair.DataEntry;
import com.taobao.tair.Result;
import com.taobao.tair.ResultCode;
import com.taobao.tair.impl.DefaultTairManager;


/**
 * 读写tair所需要的集群信息，如masterConfigServer/slaveConfigServer地址/
 * group 、namespace我们都会在正式提交代码前告知选手
 */
public class TairOperatorImpl {
	private static final Logger LOG = LoggerFactory.getLogger(Count.class);
	public String mMasterConfigServer; // tair master
	public String mSlaveConfigServer;   // tair slave
	public String mGroupName;            // group name
	public int mNamespace;                 // namespace
	
	public  DefaultTairManager mTairManager;

    public TairOperatorImpl(String masterConfigServer,
                            String slaveConfigServer,
                            String groupName,
                            int namespace) {
    	this.mMasterConfigServer = masterConfigServer;
    	this.mSlaveConfigServer = slaveConfigServer;
    	this.mGroupName = groupName;
    	this.mNamespace = namespace;
        
    }
    
    public void initTair(){
    	// 创建config server列表  
        List<String> confServers = new ArrayList<String>();  
        confServers.add(mMasterConfigServer);   
        confServers.add(mSlaveConfigServer); // 可选  
  
        // 创建客户端实例  
        mTairManager = new DefaultTairManager();  
        mTairManager.setConfigServerList(confServers);  
  
        // 设置组名  
        mTairManager.setGroupName(mGroupName);  
        // 初始化客户端  
        mTairManager.init();         
       
    }

    public boolean write(Serializable key, Serializable value) {
    	// put key-value into tair  
       
        // 第一个参数是namespace，第二个是key，第三是value，第四个是版本，第五个是有效时间  
    	int version = 0;
    	int expireTime = 1000 * 60 * 60;
        ResultCode result = mTairManager.put(mNamespace, key, value, version, expireTime);//version为0，即不关心版本；expireTime为0，即不失效
        return result.isSuccess();       
    }

    public Object get(Serializable key) {
        // 第一个参数是namespce，第二个是key  
        Result<DataEntry> result = mTairManager.get(mNamespace, key);  
        if (result.isSuccess()) {  
            DataEntry entry = result.getValue();  
            if (entry != null) {  
                // 数据存在  
                return entry.getValue();  
            } else {  
                // 数据不存在  
                return null;  
            }  
        } else {  
            // 异常处理  
            return null;
        }  
    }

    public boolean remove(Serializable key) {
        return false;
    }

    public void close(){
    	mTairManager.close();
    }
    
    public static void main(String[] args) {
    	TairOperatorImpl tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
                RaceConfig.TairGroup, RaceConfig.TairNamespace);
		tairOperator.initTair();
		String readKey = "platformTaobao_44761r8nkd_1467200520";
		String writeKey = "platformTaobao_44761r8nkd_1467200520";
		Double writeVal = 10.23;
		Double readVal = (Double) tairOperator.get(readKey);
		if(readVal==null)
			System.out.println(readKey + " not found");
		else
			System.out.println("key:" + readKey + " val:"+readVal);
		boolean ret = tairOperator.write(writeKey, writeVal);
		System.out.println("write to tair:" + ret);
    }
    
}
