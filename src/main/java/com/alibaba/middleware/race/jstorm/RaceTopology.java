package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.alibaba.middleware.race.RaceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
/**
 * 杩欐槸涓�涓緢绠�鍗曠殑渚嬪瓙
 * 閫夋墜鐨勬嫇鎵戞彁浜ゅ埌闆嗙兢锛屾垜浠槸鏈夎秴鏃惰缃殑銆傛瘡涓�夋墜鐨勬嫇鎵戞渶澶氳窇20鍒嗛挓锛屼竴鏃﹁秴杩囪繖涓椂闂�
 * 鎴戜滑浼氬皢閫夋墜鎷撴墤鏉�鎺夈��
 */

/**
 * 閫夋墜鎷撴墤鍏ュ彛绫伙紝鎴戜滑瀹氫箟蹇呴』鏄痗om.alibaba.middleware.race.jstorm.RaceTopology
 * 鍥犱负鎴戜滑鍚庡彴瀵归�夋墜鐨刧it杩涜涓嬭浇鎵撳寘锛屾嫇鎵戣繍琛岀殑鍏ュ彛绫婚粯璁ゆ槸com.alibaba.middleware.race.jstorm.RaceTopology锛�
 * 鎵�浠ヨ繖涓富绫昏矾寰勪竴瀹氳姝ｇ‘
 */
public class RaceTopology {

    private static Logger LOG = LoggerFactory.getLogger(RaceTopology.class);


    public static void main(String[] args) throws Exception {
    	Config conf = new Config();
    	conf.setNumWorkers(4);
    	conf.setNumAckers(0);
        int spout_Parallelism_hint = 4;
        int deserialize_parallel = 12;
        int split_Parallelism_hint = 8;
        int tm_Parallelism_hint = 16;
        int tb_Parallelism_hint = 16;
        int pay_Parallelism_hint = 1;

        TopologyBuilder builder = new TopologyBuilder();
        

        builder.setSpout("MetaSpout", new MetaSpout(), spout_Parallelism_hint);
        
        builder.setBolt("DeSerializeBolt", new DeSerializeBolt(), deserialize_parallel)
					.localOrShuffleGrouping("MetaSpout");
        
        builder.setBolt("SplitPaymentBolt", new SplitPaymentBolt(), split_Parallelism_hint)
						.fieldsGrouping("DeSerializeBolt", new Fields("orderid"));

        builder.setBolt("Taobao", new TaobaoBolt(), tb_Parallelism_hint)
        				.fieldsGrouping("SplitPaymentBolt", RaceConfig.TB_STREAM_ID, new Fields("hash"))
//        				.localOrShuffleGrouping("SplitPaymentBolt", RaceConfig.TB_STREAM_ID)
        				.allGrouping("SplitPaymentBolt", RaceConfig.STOP_STREAM_ID);
        
        builder.setBolt("Tmall", new TmallBolt(), tm_Parallelism_hint)
			        .fieldsGrouping("SplitPaymentBolt", RaceConfig.TM_STREAM_ID, new Fields("hash"))
//        			.localOrShuffleGrouping("SplitPaymentBolt", RaceConfig.TM_STREAM_ID)
					.allGrouping("SplitPaymentBolt", RaceConfig.STOP_STREAM_ID);
        // 由于Ratio需要历史值，所以现在这个逻辑不能并行
        builder.setBolt("Ratio", new RatioBolt(), pay_Parallelism_hint)
		        .localOrShuffleGrouping("SplitPaymentBolt", RaceConfig.PAY_STREAM_ID)
				.allGrouping("SplitPaymentBolt", RaceConfig.STOP_STREAM_ID);
        
        String topologyName = RaceConfig.JstormTopologyName;

        try {
            StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}