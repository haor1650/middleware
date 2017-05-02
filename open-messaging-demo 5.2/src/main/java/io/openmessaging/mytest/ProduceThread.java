package io.openmessaging.mytest;

import io.openmessaging.Message;
import io.openmessaging.Producer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * 发送消息线程
 * @author Administrator
 *
 */
public class ProduceThread implements Runnable {

	private String[] bucketsArray;

	private Map<String, List<Message>> messageListMap;

	private Producer producer;
	
	private CountDownLatch count ;

	/**
	 * 构造器
	 * 
	 * @param bucketsArray
	 *            指定bucket数组
	 * @param messageListMap
	 *            bucket-消息列表 映射
	 * @param producer
	 *            生产者
	 */
	public ProduceThread(String[] bucketsArray,
			Map<String, List<Message>> messageListMap, Producer producer, CountDownLatch count) {
		super();
		this.bucketsArray = bucketsArray;
		this.messageListMap = messageListMap;
		this.producer = producer;
		this.count = count;
	}

	public void run() {
		for (int i = 0; i < bucketsArray.length; i++) {
			List<Message> list = messageListMap.get(bucketsArray[i]);
			for (Message msg : list) {
				producer.send(msg);
			}
		}
		count.countDown();
	}
}