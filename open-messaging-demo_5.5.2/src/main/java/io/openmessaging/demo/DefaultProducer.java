package io.openmessaging.demo;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import io.openmessaging.BatchToPartition;
import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageFactory;
import io.openmessaging.MessageHeader;
import io.openmessaging.Producer;
import io.openmessaging.Promise;

public class DefaultProducer implements Producer {
	
	//单线程将msgJsonQueue队列中的消息内容写入commitlog文件，并将位置信息加入offsetQueue队列
	private static CommitLogWriteThread commitLogWriteThread;
	
	//单线程从offsetQueue取数据，多线程写入commitqueue文件
	private static CommitQueueWriteThread commitQueueWriteThread;
	
	//保存消息json的队列
	private static Queue<byte[]> msgJsonQueue = new ConcurrentLinkedQueue<>();//TODO 需要控制队列长度 new LinkedBlockingQueue没有明显改善
	
	//保存消息offset和length的队列，元素 = offset << 20 | jsonLength
	private static Queue<Long> offsetAndLengthQueue = new ConcurrentLinkedQueue<>();
	
	//保存消息的bucket消息
	private static Queue<String> bucketQueue = new ConcurrentLinkedQueue<>();
	
	//commitLogWriteThread和commitQueueWriteThread 是否启动
	private static boolean isStart = false;
	
	private static Lock initLock = new ReentrantLock();
	
	//确保bucketQueue和msgJsonQueue同步poll
	private static Lock pollLock = new ReentrantLock();
	
    private MessageFactory messageFactory = new DefaultMessageFactory();

    private KeyValue properties;
    
//    private static Semaphore semaphore = new Semaphore(4);
    
    public DefaultProducer(KeyValue properties) {
    	
        this.properties = properties;
        //启动 commitLogWriteThread和commitQueueWriteThread
        try {
            initLock.lock();
            if(isStart == false){
            	String storePath = properties.getString("STORE_PATH");
            	commitLogWriteThread = new CommitLogWriteThread(storePath, msgJsonQueue, offsetAndLengthQueue);
            	commitQueueWriteThread = new CommitQueueWriteThread(offsetAndLengthQueue, storePath, commitLogWriteThread, bucketQueue, pollLock);
            	Thread commitlogThread = new Thread(commitLogWriteThread);
            	commitlogThread.start();
            	new Thread(commitQueueWriteThread).start();
            	isStart = true;
            }
		} catch (Exception e) {
			e.printStackTrace();
		} finally{
			initLock.unlock();
		}
    }

    @Override public BytesMessage createBytesMessageToTopic(String topic, byte[] body) {
        return messageFactory.createBytesMessageToTopic(topic, body);
    }

    @Override public BytesMessage createBytesMessageToQueue(String queue, byte[] body) {
        return messageFactory.createBytesMessageToQueue(queue, body);
    }

    @Override public void start() {

    }

    @Override public void shutdown() {

    }

    @Override public KeyValue properties() {
        return properties;
    }

    @Override public void send(Message message) {
        if (message == null) throw new ClientOMSException("Message should not be null");
        String topic = message.headers().getString(MessageHeader.TOPIC);
        String queue = message.headers().getString(MessageHeader.QUEUE);
        if ((topic == null && queue == null) || (topic != null && queue != null)) {
            throw new ClientOMSException(String.format("Queue:%s Topic:%s should put one and only one", true, queue));
        }
        byte[] json = createJson(message);	//TODO 耗时25%
        //TODO 以下两行需要同步进行
        synchronized (DefaultProducer.class) {
            msgJsonQueue.offer(json);
            bucketQueue.offer(topic == null ? queue : topic);
		}
    }
    
    /**
     * 根据消息信息创建json对象
     * @param message
     * @param bucket
     * @return
     */
    private byte[] createJson(Message message){
    	DefaultBytesMessage bytesMessage = (DefaultBytesMessage)message;
    	byte[] jsonBytes = DefaultBytesMessage2JsonUtils.message2JsonByteArray(bytesMessage);
    	return jsonBytes;
    }

    @Override public void send(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public Promise<Void> sendAsync(Message message) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public Promise<Void> sendAsync(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void sendOneway(Message message) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void sendOneway(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

	@Override
	public BatchToPartition createBatchToPartition(String partitionName) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BatchToPartition createBatchToPartition(String partitionName,
			KeyValue properties) {
		return null;
	}
}
