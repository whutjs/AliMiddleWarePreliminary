package com.alibaba.middleware.race;

import java.io.Serializable;

public class RaceConfig implements Serializable {
	
	public static String TB_STREAM_ID = "TB_STREAM";
	public static String TM_STREAM_ID = "TM_STREAM";
	public static String PAY_STREAM_ID = "PAY_STREAM";
	public static String STOP_STREAM_ID = "STOP_STREAM";

	public static String team_code = "44761r8nkd";
    //杩欎簺鏄啓tair key鐨勫墠缂�
    public static String prex_tmall = "platformTmall_";
    public static String prex_taobao = "platformTaobao_";
    public static String prex_ratio = "ratio_";


    //杩欎簺jstorm/rocketMq/tair 鐨勯泦缇ら厤缃俊鎭紝杩欎簺閰嶇疆淇℃伅鍦ㄦ寮忔彁浜や唬鐮佸墠浼氳鍏竷
    public static String JstormTopologyName = "44761r8nkd";
    public static String MetaConsumerGroup = "44761r8nkd";
    public static String MqPayTopic = "MiddlewareRaceTestData_Pay";
    public static String MqTmallTradeTopic = "MiddlewareRaceTestData_TMOrder";
    public static String MqTaobaoTradeTopic = "MiddlewareRaceTestData_TBOrder";
//    public static String TairConfigServer = "192.168.122.212:5198";	// 本地配置
//    public static String TairSalveConfigServer = "192.168.122.212:5198"; // 本地配置
    public static String TairConfigServer = "10.101.72.127:5198";
    public static String TairSalveConfigServer = "10.101.72.128:5198";
    
    public static String TairGroup = "group_tianchi";
//    public static Integer TairNamespace = 1;  // 本地版本
    public static Integer TairNamespace = 34757;
}
