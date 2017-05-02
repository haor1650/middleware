package io.openmessaging.demo;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 该线程采用单线程的方式将所有queue和topic的消息Json字符串，写入一个commitLog文件
 * @author Administrator
 *
 */
public class CommitLogWriteThread implements Runnable{

	//该队列保存待写入文件的消息Json，需保证线程安全
	//TODO 尝试换用BlockingQueue进行优化
	private Queue<BytesMessageJson> jsonQueue;
	
	//该队列保存待写入commitqueue的消息offset，需保证线程安全
	private Queue<BytesMessageOffset> offsetQueue;
	
	//写入文件类
	private CommitLogFileStorageByMMF fileStorage;
	
	private boolean isStart = false;
	private volatile boolean end = false;
	
//	private AtomicInteger count = new AtomicInteger(0);
	
	//当jsonQueue为空时，进入等待，计时变量，超出值时线程结束
	private int waitForMsgCount = 0;// = new AtomicInteger(0);
	

	/**
	 * 原则上应该使用单例
	 * @param storePath
	 * @param jsonQueue
	 * @param offsetQueue
	 */
	public CommitLogWriteThread(String storePath, Queue<BytesMessageJson> jsonQueue, Queue<BytesMessageOffset> offsetQueue) {
		this.fileStorage = new CommitLogFileStorageByMMF(storePath);
		this.jsonQueue = jsonQueue;
		this.offsetQueue = offsetQueue;
	}
	
	BytesMessageOffset bytesMessageOffset = new BytesMessageOffset(1,	//TODO for test
			10, "q");
	
	@Override
	public void run() {
		while(true){
			BytesMessageJson messageJson = jsonQueue.poll();
			if(messageJson != null){
//				count.incrementAndGet();
				isStart = true;
				waitForMsgCount = 0;
				int offset = fileStorage.storeCommitLog(messageJson.getJson());			//耗时25%
//				int offset = 0;				//TODO for test
				BytesMessageOffset bytesMessageOffset = new BytesMessageOffset(offset,	//TODO 耗时操作，耗时25%
						messageJson.getJson().length, messageJson.getBucket());			
				offsetQueue.offer(bytesMessageOffset);
			}else{
				try {
					Thread.sleep(1);
					if(waitForMsgCount ++ > Constants.WAIT_FOR_MESSAGE_TIME){
						end = true;
						break;
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
//				if(isStart == true){
//					System.out.println("Commit Log Produce "+ count);
//					break;
//				}
			}
		}
//		fileStorage.closeBuffer();
	}

	public boolean isEnd() {
		return end;
	}
}
