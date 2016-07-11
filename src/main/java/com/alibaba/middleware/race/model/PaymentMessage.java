package com.alibaba.middleware.race.model;

import java.io.Serializable;
import java.util.Random;


/**
 * 鎴戜滑鍚庡彴RocketMq瀛樺偍鐨勪氦鏄撴秷鎭ā鍨嬬被浼间簬PaymentMessage锛岄�夋墜涔熷彲浠ヨ嚜瀹氫箟
 * 璁㈠崟娑堟伅妯″瀷锛屽彧瑕佹ā鍨嬩腑鍚勪釜瀛楁鐨勭被鍨嬪拰椤哄簭鍜孭aymentMessage涓�鏍凤紝鍗冲彲鐢↘ryo
 * 鍙嶅簭鍒楀嚭娑堟伅
 */

public class PaymentMessage implements Serializable{

    private static final long serialVersionUID = -4721410670774102273L;

    private long orderId; //璁㈠崟ID

    private double payAmount; //閲戦

    /**
     * Money鏉ユ簮
     * 0,鏀粯瀹�
     * 1,绾㈠寘鎴栦唬閲戝埜
     * 2,閾惰仈
     * 3,鍏朵粬
     */
    private short paySource; //鏉ユ簮

    /**
     * 鏀粯骞冲彴
     * 0锛宲C
     * 1锛屾棤绾�
     */
    private short payPlatform; //鏀粯骞冲彴

    /**
     * 浠樻璁板綍鍒涘缓鏃堕棿
     */
    private long createTime; //13浣嶆暟锛屾绉掔骇鏃堕棿鎴筹紝鍒濊禌瑕佹眰鐨勬椂闂撮兘鏄寚璇ユ椂闂�

    //Kryo榛樿闇�瑕佹棤鍙傛暟鏋勯�犲嚱鏁�
    public PaymentMessage() {
    }

    private static Random rand = new Random();

    public static PaymentMessage[] createPayMentMsg(OrderMessage orderMessage) {
    	 int max = (1000*60)*5;
         int min = (1000*60);
        PaymentMessage [] list = new PaymentMessage[2];
        for (short i = 0; i < 2; i++) {
            PaymentMessage msg = new PaymentMessage();
            msg.orderId = orderMessage.getOrderId();
            msg.paySource = i;
            msg.payPlatform = (short) (i % 2);
            msg.createTime = orderMessage.getCreateTime() + rand.nextInt(max)%(max-min+1) + min;;
            msg.payAmount = 0.0;
            list[i] = msg;
        }

        list[0].payAmount = rand.nextInt((int) (orderMessage.getTotalPrice() / 2));
        list[1].payAmount = orderMessage.getTotalPrice() - list[0].payAmount;

        return list;
    }

    @Override
    public String toString() {
        return "PaymentMessage{" +
                "orderId=" + orderId +
                ", payAmount=" + payAmount +
                ", paySource=" + paySource +
                ", payPlatform=" + payPlatform +
                ", createTime=" + createTime +
                '}';
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

    public short getPaySource() {
        return paySource;
    }

    public void setPaySource(short paySource) {
        this.paySource = paySource;
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
}
