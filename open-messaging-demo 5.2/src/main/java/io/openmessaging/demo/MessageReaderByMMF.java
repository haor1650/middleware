package io.openmessaging.demo;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
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
	private MappedByteBuffer commitLogBuffer;
	
	private ThreadPoolExecutor executor;
	
	private String storePath;
	//记录各bucket是否完成消费，当完成时，counter = 0
	public AtomicInteger endCounter = new AtomicInteger(0);
	
	public MessageReaderByMMF(String storePath, Queue<DefaultBytesMessage> messageQueue, 
			MappedByteBuffer commitLogBuffer, ThreadPoolExecutor executor){
		this.storePath = storePath;
		this.messageQueue = messageQueue;
		this.executor = executor;
		this.commitLogBuffer = commitLogBuffer;
	}
	
	/**
	 * 注册到bucket，开始读取并存入消息队列
	 * @param bucket
	 */
	public void registBucket(String bucket){
		//结束计数器加1
		endCounter.incrementAndGet();
//		System.out.println("End Counter "+endCounter.get());
		BucketMessageReaderByMMF mmf = new BucketMessageReaderByMMF(bucket);
		executor.submit(mmf);
	}
	public void registBucket(Set<String> bucketSet){
		for(String bucket: bucketSet){
			registBucket(bucket);
//			BucketMessageReaderByMMF mmf = new BucketMessageReaderByMMF(bucket);
//			executor.submit(mmf);
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
		private Queue<BytesMessageOffset> offsetQueue = new LinkedList<>();
		
		public BucketMessageReaderByMMF(String bucket){
			this.bucket = bucket;
			commitQueueBuffer = createCommitQueueBuffer();
		}
		
		@Override
		public void run() {
			
			//TODO 先将commitqueue全部读出，再从commitlog读数据？
			readCommitQueue();
//			System.out.println("offsetQueue:"+offsetQueue.size());//
			//TODO test
			while(true){
				BytesMessageOffset offset = offsetQueue.poll();
				if(offset != null){
					DefaultBytesMessage message = readCommitLog(offset);
					messageQueue.offer(message);
				}else{
//					Thread.yield();
					//结束计数器减1
					closeBuffer();
					endCounter.decrementAndGet();
//					System.out.println("End Counter "+endCounter.get());
					break;
				}
			}
		}
		
		/**
		 * 根据offset信息去读commitlog，并返回消息实例
		 * @param offset
		 * @return
		 */
		private DefaultBytesMessage readCommitLog(BytesMessageOffset offset){
			byte[] jsonArray = new byte[offset.getLength()];
			int index = 0;
			for(int i = offset.getOffset() ; i < offset.getOffset() + offset.getLength() ; i ++){
				jsonArray[index ++] = commitLogBuffer.get(i);
			}
			return DefaultBytesMessage2JsonUtils.byteArray2Message(jsonArray);
		}
		
		/**
		 * 完成commitqueue读取
		 */
		private void readCommitQueue(){
			int offset = 0;
			int length = 0;
			while(true){
				offset = commitQueueBuffer.getInt();
				length = commitQueueBuffer.getInt();
				//判断文件结束
				if(length == 0){
					break;
				}
				offsetQueue.offer(new BytesMessageOffset(offset, length, bucket));
			}
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
