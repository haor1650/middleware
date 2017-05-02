package io.openmessaging.demo;

import java.nio.MappedByteBuffer;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentHashMap.KeySetView;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * TODO 启用单线程将消息的offset信息写入commitqueue文件
 * @author Administrator
 *
 */
public class CommitQueueWriteThread implements Runnable{
	
	//待写入commitqueue的消息offset队列
	private Queue<BytesMessageOffset> offsetQueue;
	
//	//map将offset信息分类，每个子队列保存同一bucket的消息offset
//	private ConcurrentHashMap<String, Queue<BytesMessageOffset>> offsetMap = new ConcurrentHashMap<>();
	
	//key:bucket, value:storage
	private Map<String, CommitQueueFileStorageByMMF> storageMap = new ConcurrentHashMap<>();
	
	private String storePath;
	
//	private boolean isStart = false;
	
	private AtomicInteger count = new AtomicInteger(0);
	
	private CommitLogWriteThread commitLogWriter;
	
//	//TODO 参数优化
//	//多线程写入commitqueue文件线程池
//	private ThreadPoolExecutor executor;
	
	public CommitQueueWriteThread(Queue<BytesMessageOffset> offsetQueue, String storePath, CommitLogWriteThread commitLogWriter){
		this.offsetQueue = offsetQueue;
		this.storePath = storePath;
		this.commitLogWriter = commitLogWriter;
	}

	@Override
	public void run() {
		while(true){
			BytesMessageOffset offset = offsetQueue.poll();
			if(offset != null){
				count.incrementAndGet();
//				isStart = true;
				String bucket = offset.getBucket();
				CommitQueueFileStorageByMMF storage = storageMap.get(bucket);
				if(storage == null){
					storage = new CommitQueueFileStorageByMMF(storePath);
					storageMap.put(bucket, storage);
				}
				storage.storeCommitQueue(offset);
			}else{
				if(commitLogWriter.isEnd()){
					System.out.println("Commit Queue "+count.get());
					break;
				}else{
					Thread.yield();
				}
//				if(isStart == true){
//					System.out.println("Commit Queue "+count);
//					break;
//				}
			}
		}
		//TODO 待释放资源
	}
	
	/**
	 * 开启多个内部类线程，按bucket分类写入offset信息
	 * @author Administrator
	 *
	 */
	@Deprecated
	private class CommitQueueBucketWriteThread implements Runnable{
		
		private Queue<BytesMessageOffset> bucketOffsetQueue;
		
		private CommitQueueFileStorageByMMF fileStorage;
		
		private CommitQueueBucketWriteThread(Queue<BytesMessageOffset> bucketOffsetQueue){
			this.bucketOffsetQueue = bucketOffsetQueue;
			fileStorage = new CommitQueueFileStorageByMMF(storePath);
		}

		@Override
		//TODO 是否有必要每次都写入？
		public void run() {
			while(true){
				BytesMessageOffset offset = bucketOffsetQueue.poll();
				if(offset != null){
					fileStorage.storeCommitQueue(offset);
				}else{
					Thread.yield();
				}
			}
		}
		
		public void close(){
			fileStorage.closeBuffer();
		}
		
	}

}

