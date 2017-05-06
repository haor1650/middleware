package io.openmessaging.demo;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

/**
 * TODO 启用单线程将消息的offset信息写入commitqueue文件
 * @author Administrator
 */
public class CommitQueueWriteThread implements Runnable{
	
	//待写入commitqueue的消息offset队列
	private Queue<Long> offsetAndLengthQueue;
	private Queue<String> bucketQueue;
	
//	//map将offset信息分类，每个子队列保存同一bucket的消息offset
//	private ConcurrentHashMap<String, Queue<BytesMessageOffset>> offsetMap = new ConcurrentHashMap<>();
	
	//key:bucket, value:storage
	private Map<String, CommitQueueFileStorageByMMF> storageMap = new ConcurrentHashMap<>();
	
	private String storePath;
	
//	private boolean isStart = false;
	
	private AtomicInteger count = new AtomicInteger(0);
	
	private CommitLogWriteThread commitLogWriter;
	
	private Lock pollLock;
	
	public CommitQueueWriteThread(Queue<Long> offsetAndLengthQueue, String storePath, CommitLogWriteThread commitLogWriter, 
			Queue<String> bucketQueue, Lock pollLock){
		this.offsetAndLengthQueue = offsetAndLengthQueue;
		this.storePath = storePath;
		this.commitLogWriter = commitLogWriter;
		this.bucketQueue = bucketQueue;
		this.pollLock = pollLock;
	}

	@Override
	public void run() {
		while(true){
			//TODO offsetAndLengthQueue 与 bucketQueue 队列同步取操作
			try {
//				pollLock.lock();//TODO 单线程加锁不起作用
				if(!bucketQueue.isEmpty() && !offsetAndLengthQueue.isEmpty()){
					Long offsetAndLength = offsetAndLengthQueue.poll();
//					if(offsetAndLength != null && !bucketQueue.isEmpty()){
					count.incrementAndGet();
					String bucket = bucketQueue.poll();
					CommitQueueFileStorageByMMF storage = storageMap.get(bucket);
					if(storage == null){
						storage = new CommitQueueFileStorageByMMF(storePath);
						storageMap.put(bucket, storage);
					}
					storage.storeCommitQueue(offsetAndLength, bucket);
				}else{
					if(commitLogWriter.isEnd()){
						System.out.println("Commit Queue count "+count.get());
						break;
					}else{
						Thread.yield();
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally{
//				pollLock.unlock();
			}
		}
		//TODO 待释放资源
	}
	
	/**
	 * 开启多个内部类线程，按bucket分类写入offset信息
	 * @author Administrator
	 *
	 */
//	@Deprecated
//	private class CommitQueueBucketWriteThread implements Runnable{
//		
//		private Queue<BytesMessageOffset> bucketOffsetQueue;
//		
//		private CommitQueueFileStorageByMMF fileStorage;
//		
//		private CommitQueueBucketWriteThread(Queue<BytesMessageOffset> bucketOffsetQueue){
//			this.bucketOffsetQueue = bucketOffsetQueue;
//			fileStorage = new CommitQueueFileStorageByMMF(storePath);
//		}
//
//		@Override
//		//TODO 是否有必要每次都写入？
//		public void run() {
////			while(true){
////				BytesMessageOffset offset = bucketOffsetQueue.poll();
////				if(offset != null){
////					fileStorage.storeCommitQueue(offset);
////				}else{
////					Thread.yield();
////				}
////			}
//		}
//		
//		public void close(){
//			fileStorage.closeBuffer();
//		}
//		
//	}

}

