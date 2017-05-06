package io.openmessaging.demo;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 通过内存文件映射，启动多线程读取commitqueue和commitlog文件，每个bucket由一个线程负责，将读取到Json转为Message对象，加入队列中
 * @author Administrator
 */
public class MessageReaderByMMF {
	
	//consumer对应的消息缓冲队列，将读到的消息放入该队列中
	private Queue<DefaultBytesMessage> messageQueue;
	
	//TODO 没有考虑多个文件的情况 没有考虑线程安全问题
	private MappedByteBuffer[] commitLogBuffers;
	
	private ThreadPoolExecutor executor;
	
	private String storePath;
	//记录各bucket是否完成消费，当完成时，counter = 0
	public AtomicInteger endCounter = new AtomicInteger(0);
	
	public MessageReaderByMMF(String storePath, Queue<DefaultBytesMessage> messageQueue, 
			MappedByteBuffer[] commitLogBuffers, ThreadPoolExecutor executor){
		this.storePath = storePath;
		this.messageQueue = messageQueue;
		this.executor = executor;
		this.commitLogBuffers = commitLogBuffers;
	}
	
	/**
	 * 注册到bucket，开始读取并存入消息队列
	 * @param bucket
	 */
	public void registBucket(String bucket){
		//结束计数器加1
		endCounter.incrementAndGet();
		BucketMessageReaderByMMF mmf = new BucketMessageReaderByMMF(bucket);
		executor.execute(mmf);
	}
	public void registBucket(Set<String> bucketSet){
		for(String bucket: bucketSet){
			registBucket(bucket);
		}
	}
	/**
	 * 内部类负责一个bucket的读取
	 * @author Administrator
	 *
	 */
	private class BucketMessageReaderByMMF implements Runnable{
		
		private String bucket;
		
		//commitqueue 文件 buffer 对象 
		private MappedByteBuffer commitQueueBuffer;
		
		//将commitqueue文件读出的offset信息存入队列，不要求线程安全
		private Queue<Long> offsetAndLengthQueue = new LinkedList<>();
		
		public BucketMessageReaderByMMF(String bucket){
			this.bucket = bucket;
			commitQueueBuffer = createCommitQueueBuffer();
		}
		
		@Override
		public void run() {
			while(true){
				int readCommitQueueCount = readCommitQueue();//一次仅读出部分数据
				if(readCommitQueueCount == 0){
					break;
				}
//				System.out.println(bucket + " read commit queue done, the queue size is "+ readCommitQueueCount);
				while(readCommitQueueCount > 0){
					Long offsetAndLength = offsetAndLengthQueue.poll();
					if(offsetAndLength != null){
						DefaultBytesMessage message = readCommitLog(offsetAndLength);
						messageQueue.offer(message);
					}
					readCommitQueueCount --;
				}
			}
			//结束计数器减1
			closeBuffer();
			endCounter.decrementAndGet();
			
//			System.out.println(bucket + " read commit log done");
			
		}
		
		/**
		 * 根据offset信息去读commitlog，并返回消息实例
		 * @param offset
		 * @return
		 */
		private DefaultBytesMessage readCommitLog(long offsetAndLength){
			int length = getMsgLength(offsetAndLength);
			long fullOffset = getFullOffset(offsetAndLength);	
			int localOffset = (int)getLocalOffset(fullOffset);
			int commitLogIndex = getCommitLogIndex(fullOffset);
			byte[] jsonArray = new byte[length];
			int i = 0;
			int end = localOffset + length;
			MappedByteBuffer commitLogBuffer = null;
			try {
				commitLogBuffer = commitLogBuffers[commitLogIndex];
			} catch (RuntimeException e) {
				System.out.println("Exception offsetAndLength: " + offsetAndLength);
				System.out.println("Exception fullOffset: " + fullOffset);
				System.out.println("Exception commitLogIndex: " + (fullOffset >> Constants.COMMIT_LOG_FILE_OFFSET_BYTES));
				throw e;
			}
			for(int cursor = localOffset ; cursor < end ; cursor ++){
				jsonArray[i ++] = commitLogBuffer.get(cursor);//TODO get方法只支持int类型
			}
			return DefaultBytesMessage2JsonUtils.byteArray2Message(jsonArray);
		}
		
		/**
		 * 获得全局offset
		 * @param offsetAndLength
		 * @return
		 */
		private long getFullOffset(long offsetAndLength){
			return (offsetAndLength >>> Constants.LENGH_BYTES);
		}
		
		/**
		 * 获得commitlog文件编号
		 * @param offsetAndLength
		 * @return
		 */
		private int getCommitLogIndex(long fullOffset){
			return (int) fullOffset >>> Constants.COMMIT_LOG_FILE_OFFSET_BYTES;
		}
		
		/**
		 * 获得文件内offset
		 * @param fullOffset
		 * @return
		 */
		private long getLocalOffset(long fullOffset){
			return fullOffset & Constants.COMMIT_LOG_FILE_OFFSET_PLACEHOLDER;
		}
		
		private int getMsgLength(long offsetAndLength){
			return (int) (offsetAndLength & Constants.LENGH_PLACEHOLDER);
		}
		
		/**
		 * 完成commitqueue读取
		 * TODO 耗时操作，多线程优化？
		 */
		private int readCommitQueue(){
			long offsetAndLength = 0;
			int count = 0;
			while(count < Constants.COMMIT_QUEUE_READ_DOZEN){
				offsetAndLength = commitQueueBuffer.getLong();
				//判断文件结束
				if(offsetAndLength == 0){
					break;
				}
				offsetAndLengthQueue.offer(offsetAndLength);
				count ++;
			}
			return count;
		}
		
		private MappedByteBuffer createCommitQueueBuffer(){
			MappedByteBuffer buffer = null;
			try {
				String fullPathName = storePath + File.separator + Constants.COMMITQUEUE_FOLDER_NAME + 
						File.separator + bucket + "_offset.txt";
				buffer = MappedByteBufferCreator.createBuffer(fullPathName, Constants.COMMITQUEUE_FILE_MAX_LENGTH, false, FileChannel.MapMode.READ_ONLY);
			} catch (IOException e) {
				e.printStackTrace();
			}
			return buffer;
		}
		
		private void closeBuffer(){
			try {
				  Method getCleanerMethod = commitQueueBuffer.getClass().getMethod("cleaner", new Class[0]);  
				  getCleanerMethod.setAccessible(true);
				  sun.misc.Cleaner cleaner = (sun.misc.Cleaner)getCleanerMethod.invoke(commitQueueBuffer,new Object[0]);
				  cleaner.clean();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
	}
	
}
