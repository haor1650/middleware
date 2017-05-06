package io.openmessaging.demo;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 该线程采用单线程的方式将所有queue和topic的消息Json字符串，写入一个commitLog文件
 * @author Administrator
 *
 */
public class CommitLogWriteThread implements Runnable{

	//该队列保存待写入文件的消息Json，需保证线程安全
	//TODO 尝试换用BlockingQueue进行优化
	private Queue<byte[]> jsonQueue;
	
	//该队列保存待写入commitqueue的消息offset 元素 = offset << 20 | jsonLength，需保证线程安全
	private Queue<Long> offsetQueue;
	
	//写入文件类
	private CommitLogFileStorageByMMF fileStorage;
	
	private volatile boolean end = false;
	
	private AtomicInteger count = new AtomicInteger(0);
	
	//当jsonQueue为空时，进入等待，计时变量，超出值时线程结束
	private int waitForMsgCount = 0;// = new AtomicInteger(0);
	
	private String storePath;
	/**
	 * 原则上应该使用单例
	 * @param storePath
	 * @param jsonQueue
	 * @param offsetQueue
	 */
	public CommitLogWriteThread(String storePath, Queue<byte[]> jsonQueue, Queue<Long> offsetQueue) {
		this.storePath = storePath;
		this.fileStorage = new CommitLogFileStorageByMMF(storePath);
		this.jsonQueue = jsonQueue;
		this.offsetQueue = offsetQueue;
	}
	
	
	@Override
	public void run() {
		while(true){
			byte[] messageJson = jsonQueue.poll();
			if(messageJson != null){
				count.incrementAndGet();
				waitForMsgCount = 0;
				long offset = fileStorage.storeCommitLog(messageJson);			//耗时25%
				long offsetAndLength = getOffsetAndLength(offset, messageJson.length);		//编码操作
				offsetQueue.offer(offsetAndLength);
			}else{
				try {
					Thread.sleep(1);
					if(waitForMsgCount ++ > Constants.WAIT_FOR_MESSAGE_TIME){
						end = true;
						System.out.println("Commit log count "+count.get());
						break;
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	private long getOffsetAndLength(long offset, int length){
		long offsetAndLength = offset << Constants.LENGH_BYTES | length;		//编码操作
		return offsetAndLength;
	}

	public boolean isEnd() {
		return end;
	}
}
