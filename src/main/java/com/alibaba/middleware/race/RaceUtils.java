package com.alibaba.middleware.race;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.jstorm.Count;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;


public class RaceUtils {
	private static final Logger LOG = LoggerFactory.getLogger(RaceUtils.class);
    /**
     * 鐢变簬鎴戜滑鏄皢娑堟伅杩涜Kryo搴忓垪鍖栧悗锛屽爢绉埌RocketMq锛屾墍鏈夐�夋墜闇�瑕佷粠metaQ鑾峰彇娑堟伅锛�
     * 鍙嶅簭鍒楀嚭娑堟伅妯″瀷锛屽彧瑕佹秷鎭ā鍨嬬殑瀹氫箟绫讳技浜嶰rderMessage鍜孭aymentMessage鍗冲彲
     * @param object
     * @return
     */
    public static byte[] writeKryoObject(Object object) {
        Output output = new Output(1024);
        Kryo kryo = new Kryo();
        kryo.writeObject(output, object);
        output.flush();
        output.close();
        byte [] ret = output.toBytes();
        output.clear();
        return ret;
    }

    public static <T> T readKryoObject(Class<T> tClass, byte[] bytes) {
        Kryo kryo = new Kryo();
        Input input = new Input(bytes);
        input.close();        
        T ret = kryo.readObject(input, tClass);        
        return ret;
    }
    
    public static String getTimestamp(long timestamp){
    	Date date = new Date(timestamp);
    	DateFormat df = DateFormat.getDateTimeInstance();
    	return df.format(date);
    }
    
    public static long getTime(long timestamp){    	
		return (timestamp / 1000 / 60) * 60;
    }
}
