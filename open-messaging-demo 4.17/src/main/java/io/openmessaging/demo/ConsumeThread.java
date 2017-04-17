package io.openmessaging.demo;

import io.openmessaging.Message;
import io.openmessaging.MessageHeader;
import io.openmessaging.PullConsumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import org.junit.Assert;

public class ConsumeThread implements Callable<Integer>{
	
	private Map<String, List<Message>> dataMap;
	
	private CountDownLatch count;
	
	private PullConsumer consumer;
	
	private Map<String, Integer> offsetMap = new HashMap<>();
	
	public ConsumeThread(String queue, List<String> topics,
			Map<String, List<Message>> dataMap, CountDownLatch count, PullConsumer consumer) {
		super();
		this.dataMap = dataMap;
		this.count = count;
		this.consumer = consumer;
		
		consumer.attachQueue(queue, topics);
		offsetMap.put(queue, 0);
		for(String topic:topics){
			offsetMap.put(topic, 0);
		}
	}

	@Override
	public Integer call() throws Exception {
		
		while (true) {
			Message message = consumer.pullNoWait();
			if (message == null) {
				// 拉取为null则认为消息已经拉取完毕
				break;
			}
			String topic = message.headers().getString(MessageHeader.TOPIC);
			String queue = message.headers().getString(MessageHeader.QUEUE);
			// 实际测试时，会一一比较各个字段
			String bucket = null;
			if (topic != null) {
				bucket = topic;
			} else {
				bucket = queue;
			}
			List<Message> list = dataMap.get(bucket);
			Integer offset = offsetMap.get(bucket);
			Assert.assertEquals(list.get(offset), message);
			offsetMap.put(bucket, offset + 1);
		}
		
		count.countDown();
		int sum = 0;
		for(Integer offset: offsetMap.values()){
			sum += offset;
		}
		return sum;
	}
}
