package io.openmessaging.mytest;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageHeader;
import io.openmessaging.Producer;
import io.openmessaging.PullConsumer;
import io.openmessaging.demo.DefaultKeyValue;
import io.openmessaging.demo.DefaultProducer;
import io.openmessaging.demo.DefaultPullConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;

/**
 * 多线程进行生产和消费测试用例
 * 
 * @author Administrator
 *
 */
public class MultiThreadTester {

	/**
	 * 构造测试数据
	 * @param queues
	 * @param topics
	 * @param amount
	 * @param producer
	 * @return
	 */
	private static Map<String, List<Message>> constructTestData(
			String[] queues, String[] topics, int amount, Producer producer) {
		Map<String, List<Message>> dataMap = new HashMap<>();
		for (int i = 0; i < queues.length; i++) {
			String queue = queues[i];
			List<Message> list = new ArrayList<>(amount);
			for (int j = 0; j < amount; j++) {
				list.add(producer.createBytesMessageToQueue(queue, (queue + j).getBytes()));
			}
			dataMap.put(queue, list);
		}
		for (int i = 0; i < topics.length; i++) {
			String topic = topics[i];
			List<Message> list = new ArrayList<>(amount);
			for (int j = 0; j < amount; j++) {
				list.add(producer.createBytesMessageToTopic(topic, (topic + j).getBytes()));
			}
			dataMap.put(topic, list);
		}
		return dataMap;
	}
	
	/**
	 * 测试生产过程
	 * @param buckesList 列表中每个数组String[]作为一个线程发送
	 * @param dataMap
	 * @param executor
	 * @param producer
	 * @return 生产用时
	 */
	private static long produceTest(List<String[]> buckesList, Map<String, List<Message>> dataMap, ThreadPoolExecutor executor, Producer producer){
		CountDownLatch count = new CountDownLatch(buckesList.size());
		long start = System.currentTimeMillis();
		for(String[] buckets: buckesList){
			ProduceThread produceThread = new ProduceThread(buckets, dataMap, producer, count);
			executor.submit(produceThread);
		}
		try {
			count.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return System.currentTimeMillis() - start;
	}
	
	
	public static void main(String[] args) {
		KeyValue properties = new DefaultKeyValue();
		properties.put("STORE_PATH", "" + System.currentTimeMillis()); // 实际测试时利用
																		// STORE_PATH
																		// 传入存储路径
		//定义线程池
		BlockingQueue<Runnable> threadQueue = new ArrayBlockingQueue<>(10);
		ThreadPoolExecutor executor = new ThreadPoolExecutor(5, 10, 10L, TimeUnit.SECONDS, threadQueue);

		// 这个测试程序的测试逻辑与实际评测相似，但注意这里是单线程的，实际测试时会是多线程的，并且发送完之后会Kill进程，再起消费逻辑
		// 构造测试数据
		final Producer producer = new DefaultProducer(properties);
		String[] topics = { "TOPIC1", "TOPIC2" };
		String[] queues = { "QUEUE1", "QUEUE2" };
		Map<String, List<Message>> dataMap = constructTestData(queues, topics, 1024*10, producer);

//		String topic1 = "TOPIC1"; // 实际测试时大概会有100个Topic左右
//		String topic2 = "TOPIC2"; // 实际测试时大概会有100个Topic左右
//		String queue1 = "QUEUE1"; // 实际测试时大概会有100个Queue左右
//		String queue2 = "QUEUE2"; // 实际测试时大概会有100个Queue左右

		// 发送, 实际测试时，会用多线程来发送, 每个线程发送自己的Topic和Queue
		List<String[]> buckesList = new ArrayList<>();
		buckesList.add(topics);
		buckesList.add(queues);
		long T1 = produceTest(buckesList, dataMap, executor, producer);

		long startConsumer = System.currentTimeMillis();

		System.out.println("生产完毕");

		CountDownLatch count = new CountDownLatch(2);
		ConsumeThread consumeThread1 = new ConsumeThread(queues[0], Collections.singletonList(topics[0]), dataMap, count, new DefaultPullConsumer(properties));
		List<String> topics1 = Arrays.asList(topics);
		ConsumeThread consumeThread2 = new ConsumeThread(queues[1], topics1, dataMap, count, new DefaultPullConsumer(properties));
		//executor.submit(consumeThread1);
		Future<Integer> f1 = executor.submit(consumeThread1);
		Future<Integer> f2 = executor.submit(consumeThread2);
		
		try {
			count.await();
			long endConsumer = System.currentTimeMillis();
			long T2 = endConsumer - startConsumer;
			System.out.println(String.format("Team1 cost:%d ms tps:%d q/ms", T2
					+ T1, (f1.get() + f2.get()) / (T1 + T2)));
		} catch (Exception e) {
			e.printStackTrace();
		} finally{
			executor.shutdown();
		}
	}
}
