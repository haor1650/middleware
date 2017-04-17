package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageHeader;
import io.openmessaging.Producer;
import io.openmessaging.PullConsumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import org.junit.Assert;

public class DemoTester {


    public static void main(String[] args) {
        KeyValue properties = new DefaultKeyValue();
        properties.put("STORE_PATH", "E:\\Code\\Java Code\\Eclipse MidWare\\open-messaging-demo 4.15\\"+System.currentTimeMillis()); //实际测试时利用 STORE_PATH 传入存储路径
        //这个测试程序的测试逻辑与实际评测相似，但注意这里是单线程的，实际测试时会是多线程的，并且发送完之后会Kill进程，再起消费逻辑

        final Producer producer = new DefaultProducer(properties);
        
        //构造测试数据
        String topic1 = "TOPIC1"; //实际测试时大概会有100个Topic左右
        String topic2 = "TOPIC2"; //实际测试时大概会有100个Topic左右
        String queue1 = "QUEUE1"; //实际测试时大概会有100个Queue左右
        String queue2 = "QUEUE2"; //实际测试时大概会有100个Queue左右
        List<Message> messagesForTopic1 = new ArrayList<>(1024);
        List<Message> messagesForTopic2 = new ArrayList<>(1024);

        List<Message> messagesForQueue1 = new ArrayList<>(1024);
        List<Message> messagesForQueue2 = new ArrayList<>(1024);

        for (int i = 0; i < 1024; i++) {
            messagesForTopic1.add(producer.createBytesMessageToTopic(topic1,  (topic1 + i).getBytes()));
            messagesForTopic2.add(producer.createBytesMessageToTopic(topic2,  (topic2 + i).getBytes()));
            messagesForQueue1.add(producer.createBytesMessageToQueue(queue1, (queue1 + i).getBytes()));
            messagesForQueue2.add(producer.createBytesMessageToQueue(queue2, (queue2 + i).getBytes()));
        }

        long start = System.currentTimeMillis();
        //发送, 实际测试时，会用多线程来发送, 每个线程发送自己的Topic和Queue
        
        Thread p1 = new Thread(new Runnable() {
        	@Override
        	public void run() {
                for (int i = 0; i < 1024; i++) {
                    producer.send(messagesForTopic1.get(i));
                    producer.send(messagesForQueue1.get(i));
                }
        	}
        });
  
        Thread p2 = new Thread(new Runnable() {
        	@Override
        	public void run() {
                for (int i = 0; i < 1024; i++) {
                    producer.send(messagesForTopic2.get(i));
                    producer.send(messagesForQueue2.get(i));
                }
        	}
        });
//        for (int i = 0; i < 1024; i++) {
//            producer.send(messagesForTopic1.get(i));
//            producer.send(messagesForTopic2.get(i));
//            producer.send(messagesForQueue1.get(i));
//            producer.send(messagesForQueue2.get(i));
//        }
        p1.start();
        p2.start();
        
        try {
        	p1.join();//等待线程结束
			p2.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
        
        long end = System.currentTimeMillis();

        long T1 = end - start;
        
        long startConsumer = System.currentTimeMillis();
        
        //消费样例2，实际测试时会Kill掉发送进程，另取进程进行消费
        Callable<Integer> c1 = new Callable<Integer>() {
        	@Override
        	public Integer call() throws Exception {
	            PullConsumer consumer2 = new DefaultPullConsumer(properties);
	            List<String> topics = new ArrayList<>();
	            topics.add(topic1);
	            topics.add(topic2);
	            consumer2.attachQueue(queue2, topics);

	            int queue2Offset = 0, topic1Offset = 0, topic2Offset = 0;

//	            long startConsumer = System.currentTimeMillis();
	            while (true) {
	                Message message = consumer2.pullNoWait();
	                if (message == null) {
	                    //拉取为null则认为消息已经拉取完毕
	                    break;
	                }
	                String topic = message.headers().getString(MessageHeader.TOPIC);
	                String queue = message.headers().getString(MessageHeader.QUEUE);
	                //实际测试时，会一一比较各个字段
	                if (topic != null) {
	                    if (topic.equals(topic1)) {
	                    	//需要重写DefaultBytesMessage以及相关类的equals方法
	                        Assert.assertEquals(messagesForTopic1.get(topic1Offset++), message);
	                    } else {
	                        Assert.assertEquals(topic2, topic);
	                        Assert.assertEquals(messagesForTopic2.get(topic2Offset++), message);
	                    }
	                } else {
	                    Assert.assertEquals(queue2, queue);
	                    Assert.assertEquals(messagesForQueue2.get(queue2Offset++), message);
	                }
	            }
	            return queue2Offset + topic1Offset + topic2Offset;
        	}
		};
		
        Callable<Integer> c2 = new Callable<Integer>() {
        	@Override
        	public Integer call() throws Exception {
	            PullConsumer consumer1 = new DefaultPullConsumer(properties);
	            consumer1.attachQueue(queue1, Collections.singletonList(topic1));
	
	            int queue1Offset = 0, topic1Offset = 0;
	
	//            long startConsumer = System.currentTimeMillis();
	            while (true) {
	                Message message = consumer1.pullNoWait();
	//                System.out.println("consumer1:"+message);
	                if (message == null) {
	                    //拉取为null则认为消息已经拉取完毕
	                    break;
	                }
	                String topic = message.headers().getString(MessageHeader.TOPIC);
	                String queue = message.headers().getString(MessageHeader.QUEUE);
	                //实际测试时，会一一比较各个字段
	                if (topic != null) {
	                    Assert.assertEquals(topic1, topic);
	                    Assert.assertEquals(messagesForTopic1.get(topic1Offset++), message);
	                } else {
	                    Assert.assertEquals(queue1, queue);
	                    Assert.assertEquals(messagesForQueue1.get(queue1Offset++), message);
	                }
	            }
	            return queue1Offset + topic1Offset;
        	}
		};
		
		FutureTask<Integer> ctask1 = new FutureTask<>(c1);
		FutureTask<Integer> ctask2 = new FutureTask<>(c2);
		
		Thread cThread1 = new Thread(ctask1);
		Thread cThread2 = new Thread(ctask2);
		
		cThread1.start();
		cThread2.start();
        try {
        	cThread1.join();
        	cThread2.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
        long endConsumer = System.currentTimeMillis();
        long T2 = endConsumer - startConsumer;
        try {
			System.out.println(String.format("Team1 cost:%d ms tps:%d q/ms", T2 + T1, (ctask1.get() + ctask2.get())/(T1 + T2)));
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
    }
}
