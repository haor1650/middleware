package io.openmessaging.mytest;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import io.openmessaging.demo.CommitLogWriteThread;
import io.openmessaging.demo.CommitQueueWriteThread;

public class CommitLogWriteThreadTest {
	public static void main(String[] args) {
		String storePath = ""+System.currentTimeMillis();
		Queue<byte[]> msgJsonQueue = new ConcurrentLinkedQueue<>();
		Queue<String> bucketQueue = new ConcurrentLinkedQueue<>();
		Queue<Long> offsetAndLengthQueue = new ConcurrentLinkedQueue<>();
		for(int i = 0; i < 10000;i ++){
//			msgJsonQueue.offer(new BytesMessageJson("JsonTest".getBytes(), "queue1"));
			msgJsonQueue.offer("JsonTest".getBytes());
			bucketQueue.offer("queue1");
		}
		for(int i = 0; i < 10000;i ++){
			msgJsonQueue.offer("JsonTest".getBytes());
			bucketQueue.offer("queue2");
		}
		CommitLogWriteThread commitLogWriteThread = new CommitLogWriteThread(storePath, msgJsonQueue, offsetAndLengthQueue);
		CommitQueueWriteThread commitQueueWriteThread = new CommitQueueWriteThread(offsetAndLengthQueue, storePath, commitLogWriteThread, bucketQueue, new ReentrantLock());
		new Thread(commitLogWriteThread).start();
		new Thread(commitQueueWriteThread).start();
		try {
			Thread.sleep(1000);
			System.out.println(msgJsonQueue);
			System.out.println(offsetAndLengthQueue);
			System.out.println(bucketQueue);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
