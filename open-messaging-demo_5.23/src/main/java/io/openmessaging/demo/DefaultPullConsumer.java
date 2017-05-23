package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;
import io.openmessaging.tester.ConsumerTester;

import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DefaultPullConsumer implements PullConsumer {
	
	/**
	 * 静态变量
	 */
	private static MappedByteBuffer[] commitLogBuffers;
	private volatile static boolean initDone = false;
	
    private KeyValue properties;
    private String queue;
    //由一个队列和多个主题组成的集合
    private Set<String> buckets = new HashSet<>();
    private List<String> bucketList = new ArrayList<>();
    private MappedByteBuffer[] commitQueueBuffers;
    private int bucketIndex = 0;
    private long[] offsetAndLengthBuffer = new long[Constants.COMMIT_QUEUE_READ_DOZEN];
    private DefaultBytesMessage[] messageBuffer = new DefaultBytesMessage[Constants.COMMIT_QUEUE_READ_DOZEN];
    private int cursor = 0;
    private int cursorMax = 0;
    
    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
        synchronized (DefaultPullConsumer.class) {
			if(!initDone){
				commitLogBuffers = MappedByteBufferCreator.createCommitLogBuffers(properties.getString("STORE_PATH"));
				initDone = true;
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
        for(int i = 0; i < bucketList.size(); i ++){
        	commitQueueBuffers[i] = MappedByteBufferCreator.createCommitQueueBuffer(properties.getString("STORE_PATH"), bucketList.get(i));
        }
    }

    
    private int count = 0;
    
	@Override
	public Message poll() {
		count ++;
		if(count % (io.openmessaging.tester.Constants.PRO_MAX/10) == 0){
//			System.out.println(Thread.currentThread()+": "+(count / (io.openmessaging.tester.Constants.PRO_MAX/10))+"0%, "+(System.currentTimeMillis()- ConsumerTester.startTime)+"ms");
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
	
	private int readDozenMessages(){
		int readCommitQueueNum = 0;
		while(readCommitQueueNum == 0){
			readCommitQueueNum = readCommitQueue(commitQueueBuffers[bucketIndex]);
			if(readCommitQueueNum == 0){
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
		long offsetAndLength = 0;
		int count = 0;
		while(count < Constants.COMMIT_QUEUE_READ_DOZEN){
			offsetAndLength = commitQueueBuffer.getLong();
			//判断文件结束
			if(offsetAndLength == 0){
				break;
			}
			offsetAndLengthBuffer[count] = offsetAndLength;
			count ++;
		}
		return count;
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

}
