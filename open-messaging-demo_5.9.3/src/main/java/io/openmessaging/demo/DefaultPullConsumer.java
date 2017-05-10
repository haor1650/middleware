package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;
import io.openmessaging.tester.ConsumerTester;

import java.io.File;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class DefaultPullConsumer implements PullConsumer {
	
	/**
	 * 静态变量
	 */
	private static MappedByteBuffer[] commitLogBuffers;
	private static AtomicInteger[] commitLogBufferUsageCounter;
	private static boolean[] commitLogBufferClosed;
//	private static Lock createCommitLogLock = new ReentrantLock();
//	private static Condition createCommitLogCondition = createCommitLogLock.newCondition();
	
//	private static boolean[] commitLogBufferCreated;
	/**
	 * 非静态成员变量
	 */
	//commit log buffer对象
//	private MappedByteBuffer[] commitLogBuffers;

    private KeyValue properties;
    private String queue;
    
    //由一个队列和多个主题组成的集合
    private Set<String> buckets = new HashSet<>();
    private List<String> bucketList = new ArrayList<>();
    
    //commit queue buffer对象
    private MappedByteBuffer[] commitQueueBuffers;
    
    //记录每个commitqueue buffer更新的次数
    private int[] updateTimes;
    
    //标记当前读到哪个bucket
    private int bucketIndex = 0;
    
    //index = bucketIndex，记录各bucket消费到第几个commitlog
    private int[] bucketCommitLogIndex;
    
    //index = bucketIndex，记录各bucket是否消费完成
    private boolean[] consumeDone;
    
    //存储一次读出的commitqueue
    private byte[] commitqueueByteBuffer = new byte[Constants.COMMIT_QUEUE_READ_DOZEN << 3];
    
    //存储一次读出的commitqueue，经转码之后
    private long[] offsetAndLengthBuffer = new long[Constants.COMMIT_QUEUE_READ_DOZEN];
    
    //存储一次读出的message
    private DefaultBytesMessage[] messageBuffer = new DefaultBytesMessage[Constants.COMMIT_QUEUE_READ_DOZEN];
    
    //记录poll到第几条消息
    private int cursor = 0;
    private int cursorMax = 0;
    
    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
        synchronized (DefaultPullConsumer.class) {
        	if(commitLogBuffers == null){
        		String storePath = properties.getString("STORE_PATH");
        		File commitLogFolder = new File(storePath+File.separator+Constants.COMMITLOG_FOLDER_NAME);
        		int commitLogNum = commitLogFolder.listFiles().length;
            	commitLogBuffers = new MappedByteBuffer[commitLogNum];//MappedByteBufferCreator.createCommitLogBuffers(properties.getString("STORE_PATH"));
				commitLogBufferUsageCounter = new AtomicInteger[commitLogBuffers.length];
				commitLogBufferClosed = new boolean[commitLogBuffers.length];
//				commitLogBufferCreated = new boolean[commitLogBuffers.length];
				for(int i = 0;i < commitLogBuffers.length; i ++){
					commitLogBufferUsageCounter[i] = new AtomicInteger();
				}
				commitLogBuffers[0] = MappedByteBufferCreator.createCommitLogBuffer(properties.getString("STORE_PATH"), 1);
        	}
		}
    }

    @Override public KeyValue properties() {
        return properties;
    }

    @Override public synchronized void attachQueue(String queueName, Collection<String> topics) {
        if (queue != null && !queue.equals(queueName)) {
            throw new ClientOMSException("You have alreadly attached to a queue " + queue);
        }
        //设定该Consumer的消息队列
        queue = queueName;
        //将消息队列加入buckets
        buckets.add(queueName);
        //将topics集合加入buckets
        buckets.addAll(topics);
        bucketList.clear();
        //将所有buckets中的元素加入bucketList
        bucketList.addAll(buckets);
        
        commitQueueBuffers = new MappedByteBuffer[bucketList.size()];
        bucketCommitLogIndex = new int[bucketList.size()];
        consumeDone = new boolean[bucketList.size()];
        updateTimes = new int[bucketList.size()];
        for(int i = 0; i < bucketList.size(); i ++){
        	commitQueueBuffers[i] = MappedByteBufferCreator.createCommitQueueBuffer(properties.getString("STORE_PATH"), bucketList.get(i), 0, 
        			Constants.UPDATE_COMMIT_QUEUE_BUFFER_LENGTH);
        	
        	//初始化
        	bucketCommitLogIndex[i] = tryCommitQueueLogIndex(commitQueueBuffers[i]);
        	commitLogBufferUsageCounter[bucketCommitLogIndex[i]].incrementAndGet();
        }
    }

    /**
     * 从commitqueuebuffer中读取一个long，返回其logIndex，并将buffer复位
     * @param commitQueueBuffer
     * @return
     */
    private int tryCommitQueueLogIndex(MappedByteBuffer commitQueueBuffer){
    	long offsetAndLength = commitQueueBuffer.getLong();
    	int commitLogIndex = getCommitLogIndex(getFullOffset(offsetAndLength));
    	commitQueueBuffer.position(0);
    	return commitLogIndex;
    }

    
    private int count = 0;
    
	@Override
	public Message poll() {
		count ++;
		if(count % (io.openmessaging.tester.Constants.PRO_MAX/10) == 0){
			System.out.println(Thread.currentThread()+": "+(count / (io.openmessaging.tester.Constants.PRO_MAX/10))+"0%, "+(System.currentTimeMillis()- ConsumerTester.startTime)+"ms");
			System.out.println(Arrays.toString(commitLogBufferUsageCounter));
		}
		if(count == io.openmessaging.tester.Constants.PRO_MAX){
			return null;
		}
		if(cursor < cursorMax){
			return messageBuffer[cursor ++];
		}else{
			cursorMax = readDozenMessages();
			if(cursorMax == 0){
				return null;
			}
			cursor = 0;
			return messageBuffer[cursor ++];
		}
	}
	//TODO 费时**
	private int readDozenMessages(){
//		try {
////			Thread.sleep(1);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
		int readCommitQueueNum = 0;
		while(readCommitQueueNum == 0){
			if(!commitQueueBuffers[bucketIndex].hasRemaining()){
				updateCommitQueueBuffer();
				//重新选择bucket
				bucketIndex = selectNextBucket();
			}
			readCommitQueueNum = readCommitQueue(commitQueueBuffers[bucketIndex]);
			if(readCommitQueueNum == 0){
				consumeDone[bucketIndex] = true;
				bucketIndex ++;
				if(bucketIndex == bucketList.size()){
					return 0;
				}
			}
		}
		for(int i = 0;i < readCommitQueueNum;i ++){
			DefaultBytesMessage message = readCommitLog(offsetAndLengthBuffer[i]);
			messageBuffer[i] = message;
		}
		return readCommitQueueNum;
	}
	
	private int selectNextBucket(){
		int bucketIndex = -1;
		int lastCommitLogIndex = Integer.MAX_VALUE;
		for(int i = 0; i < bucketList.size(); i ++){
			if(!consumeDone[i]){
				if(bucketCommitLogIndex[i] < lastCommitLogIndex){
					lastCommitLogIndex = bucketCommitLogIndex[i];
					bucketIndex = i;
				}
			}
		}
//		System.out.println(Thread.currentThread() + " select bucket "+ bucketIndex + ", " +Arrays.toString(bucketCommitLogIndex));
		return bucketIndex;
	}
	
	private void updateCommitQueueBuffer(){
		//更新commitQueueBuffer
		updateTimes[bucketIndex] ++;
		closeBuffer(commitQueueBuffers[bucketIndex]);
		commitQueueBuffers[bucketIndex] = MappedByteBufferCreator.createCommitQueueBuffer(properties.getString("STORE_PATH"), bucketList.get(bucketIndex), 
				Constants.UPDATE_COMMIT_QUEUE_BUFFER_LENGTH*updateTimes[bucketIndex], 
    			Constants.UPDATE_COMMIT_QUEUE_BUFFER_LENGTH);
		System.gc();
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
		
		//更新bucketCommitLogIndex
		if(commitLogIndex != bucketCommitLogIndex[bucketIndex]){
			commitLogBufferUsageCounter[commitLogIndex].incrementAndGet();
			commitLogBufferUsageCounter[bucketCommitLogIndex[bucketIndex]].decrementAndGet();
			synchronized (DefaultPullConsumer.class) {
				clearCommitLogBuffer();
			}
			bucketCommitLogIndex[bucketIndex] = commitLogIndex;
		}
		
		byte[] jsonArray = new byte[length];
		int i = 0;
		int end = localOffset + length;
		MappedByteBuffer commitLogBuffer = null;
		
		commitLogBuffer = commitLogBuffers[commitLogIndex];
		if(commitLogBuffer == null){
			synchronized (DefaultPullConsumer.class) {
				if(commitLogBuffers[commitLogIndex] == null){
					commitLogBuffers[commitLogIndex] = MappedByteBufferCreator.createCommitLogBuffer(properties.getString("STORE_PATH"), commitLogIndex+1);
				}
			}
		}
		commitLogBuffer = commitLogBuffers[commitLogIndex];
		
		synchronized (commitLogBuffer) {
			for(int cursor = localOffset ; cursor < end ; cursor ++){
				jsonArray[i ++] = commitLogBuffer.get(cursor);//TODO get方法只支持int类型
			}
		}
		DefaultBytesMessage msg = null;
		try {
			msg = DefaultBytesMessage2JsonUtils.byteArray2Message(jsonArray);
		} catch (RuntimeException e) {
			e.printStackTrace();
			System.out.println("offsetAndLength " + offsetAndLength);
			System.out.println("length "+length +", localOffset "+localOffset+", commitLogIndex "+commitLogIndex +", fullOffset "+fullOffset);
		}
		return msg;
	}

	private void clearCommitLogBuffer(){
		for(int i = 0; i < commitLogBuffers.length; i ++){
			if(commitLogBufferUsageCounter[i].get() != 0){
				return;
			}else if(!commitLogBufferClosed[i]){
				commitLogBufferClosed[i] = true;
				closeBuffer(commitLogBuffers[i]);
				System.out.println(Arrays.toString(commitLogBufferUsageCounter) + ", close " + i);
			}
		}
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
		return (int) (fullOffset >>> Constants.COMMIT_LOG_FILE_OFFSET_BYTES);
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
	 * 完成commitqueue读取,返回读取数量
	 */
	private int readCommitQueue(MappedByteBuffer commitQueueBuffer){
		commitQueueBuffer.get(commitqueueByteBuffer);
		long offsetAndLength = bytes2Long(commitqueueByteBuffer, 0);
		int count = 0;
		while(offsetAndLength != 0){
			offsetAndLengthBuffer[count] = offsetAndLength;
			count ++;
			if(count == Constants.COMMIT_QUEUE_READ_DOZEN){
				break;
			}
			offsetAndLength = bytes2Long(commitqueueByteBuffer, count << 3);
		}
		return count;
	}
	
	private long bytes2Long(byte[] byteNum, int start) {
		long num = 0;
		for (int ix = start; ix < start + 8; ++ix) {
			num <<= 8;
			num |= (byteNum[ix] & 0xff);
		}
		return num;
	}

	@Override
	public Message poll(KeyValue properties) {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public void ack(String messageId) {
		// TODO Auto-generated method stub
	}


	@Override
	public void ack(String messageId, KeyValue properties) {
		// TODO Auto-generated method stub
	}
	
	private void closeBuffer(MappedByteBuffer buffer){
		try {
			  Method getCleanerMethod = buffer.getClass().getMethod("cleaner", new Class[0]);  
			  getCleanerMethod.setAccessible(true);
			  sun.misc.Cleaner cleaner = (sun.misc.Cleaner)getCleanerMethod.invoke(buffer,new Object[0]);
			  cleaner.clean();
			  System.gc();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
