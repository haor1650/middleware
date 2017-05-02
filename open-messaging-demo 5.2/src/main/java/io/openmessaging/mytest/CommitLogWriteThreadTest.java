package io.openmessaging.mytest;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import io.openmessaging.demo.BytesMessageJson;
import io.openmessaging.demo.BytesMessageOffset;
import io.openmessaging.demo.CommitLogWriteThread;
import io.openmessaging.demo.CommitQueueWriteThread;

public class CommitLogWriteThreadTest {
	public static void main(String[] args) {
		String storePath = ""+System.currentTimeMillis();
		ConcurrentLinkedQueue<BytesMessageJson> msgJsonQueue = new ConcurrentLinkedQueue<>();
		for(int i = 0; i < 200000;i ++){
			msgJsonQueue.offer(new BytesMessageJson("JsonTest".getBytes(), "queue1"));
		}
		for(int i = 0; i < 200000;i ++){
			msgJsonQueue.offer(new BytesMessageJson("JsonTest".getBytes(), "queue2"));
		}
		ConcurrentLinkedQueue<BytesMessageOffset> offsetQueue = new ConcurrentLinkedQueue<>();
		CommitLogWriteThread commitLogWriteThread = new CommitLogWriteThread(storePath, msgJsonQueue, offsetQueue);
		CommitQueueWriteThread commitQueueWriteThread = new CommitQueueWriteThread(offsetQueue, storePath, commitLogWriteThread);
		new Thread(commitLogWriteThread).start();
		new Thread(commitQueueWriteThread).start();
		try {
			Thread.sleep(1000);
			System.out.println(msgJsonQueue);
			System.out.println(offsetQueue);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
