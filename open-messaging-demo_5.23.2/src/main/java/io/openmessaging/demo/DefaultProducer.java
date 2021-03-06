package io.openmessaging.demo;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

//import net.sourceforge.sizeof.SizeOf;
import io.openmessaging.BatchToPartition;
import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageFactory;
import io.openmessaging.MessageHeader;
import io.openmessaging.Producer;
import io.openmessaging.Promise;

public class DefaultProducer implements Producer {
	
	/**
	 * 静态成员变量
	 */
	//写入commitlog文件对象，写入操作应保证线程安全
	private static CommitLogFileStorageByMMF commitLogWriter;
	//key：bucket	value：写入commitqueue文件对象
	private static Map<String, CommitQueueFileStorageByMMF> commitQueueWriterMap = new ConcurrentHashMap<>();
	
	//初始化完成
	private static volatile boolean initDone = false;
	
	private static long startTime = System.currentTimeMillis();
	
	/**
	 * 非静态成员变量
	 */
    private KeyValue properties;
    
    //消息缓存
    private byte[][] jsonBuffer = new byte[Constants.ONCE_WRITE_DOZEN][];
    private String[] bucketBuffer = new String[Constants.ONCE_WRITE_DOZEN];
    private int cursor = 0;
    
    private MessageFactory messageFactory = new DefaultMessageFactory();
    
    private byte[] json = new byte[Constants.JSON_MAX_LENGTH];

    public DefaultProducer(KeyValue properties) {
        this.properties = properties;
        //静态成员初始化//类同步
        synchronized (DefaultProducer.class) {
        	if(!initDone){
        		System.out.println("PRO_MAX : "+io.openmessaging.tester.Constants.PRO_MAX);
            	commitLogWriter = new CommitLogFileStorageByMMF(properties.getString("STORE_PATH"));
            	initDone = true;
    		}
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

//    private int count = 0;
    private static AtomicInteger count = new AtomicInteger();
    
    @Override public void send(Message message) {
//    	System.out.println(message);
        if (message == null) throw new ClientOMSException("Message should not be null");
        String topic = message.headers().getString(MessageHeader.TOPIC);
        String queue = message.headers().getString(MessageHeader.QUEUE);
        if ((topic == null && queue == null) || (topic != null && queue != null)) {
            throw new ClientOMSException(String.format("Queue:%s Topic:%s should put one and only one", true, queue));
        }
        int get = count.incrementAndGet();
		if(get % (io.openmessaging.tester.Constants.PRO_MAX) == 0){
			System.out.println("count "+get + " " +(System.currentTimeMillis()- startTime)+"ms");
//			long deepSizeOf = SizeOf.deepSizeOf(commitLogWriter);
//			long sizeOf = SizeOf.sizeOf(commitLogWriter);
//			System.out.println("size of commitLogWriter "+deepSizeOf+", "+sizeOf);
//			deepSizeOf = SizeOf.deepSizeOf(commitQueueWriterMap);
//			sizeOf = SizeOf.sizeOf(commitQueueWriterMap);
//			System.out.println("Size of commitQueueWriterMap "+deepSizeOf+", "+sizeOf);
//			deepSizeOf = SizeOf.deepSizeOf(jsonBuffer);
//			System.out.println("Size of jsonBuffer "+deepSizeOf);
//			deepSizeOf = SizeOf.deepSizeOf(bucketBuffer);
//			System.out.println("Size of bucketBuffer "+deepSizeOf);
		}

        storeMessage(topic == null ? queue:topic, (DefaultBytesMessage)message);
    }
    
    /**
     * 5.22 update 修改DefaultBytesMessage2JsonUtils接口
     * @param bucket
     * @param message
     */
    private void storeMessage(String bucket,DefaultBytesMessage message){
        json = DefaultBytesMessage2JsonUtils.message2JsonByteArray(message);
        jsonBuffer[cursor ] = json;
        bucketBuffer[cursor ++] = bucket;
        //写入一组
        if(cursor == Constants.ONCE_WRITE_DOZEN){
        	writeOnce(Constants.ONCE_WRITE_DOZEN);
        	cursor = 0;
        }
    }
    
    /**
     * 一次写入多条消息，锁的粗化
     */
    private long writeCount = 0;
    
    private void writeOnce(int witeDozen){
//    	long s = System.currentTimeMillis();
    	synchronized(commitLogWriter){
        	for(int i = 0; i < witeDozen; i ++ ){
        		writeCount ++;
        		byte[] json = jsonBuffer[i];
        		String bucket = bucketBuffer[i];
                long offset = commitLogWriter.storeCommitLog(json);
                long offsetAndLength = getOffsetAndLength(offset, json.length);
                CommitQueueFileStorageByMMF commitQueueWriter = null;
                while(commitQueueWriter == null){		//TODO 线程安全问题 CAS?
                	commitQueueWriter = commitQueueWriterMap.get(bucket);
                	if(commitQueueWriter != null){
                		break;
                	}
                	int mapSize = commitQueueWriterMap.size();
                	commitQueueWriter = new CommitQueueFileStorageByMMF(properties.getString("STORE_PATH"), bucket);
                	if(mapSize == commitQueueWriterMap.size()){
                		commitQueueWriterMap.put(bucket, commitQueueWriter);
//                		System.out.println("new bucket "+bucket +", sum "+commitQueueWriterMap.size());
                	}else{
                		commitQueueWriter = null;
                	}
                }
                commitQueueWriter.storeCommitQueue(offsetAndLength);
        	}
    	}
    }
    
	private long getOffsetAndLength(long offset, int length){
		long offsetAndLength = offset << Constants.LENGH_BYTES | length;		//编码操作
		return offsetAndLength;
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

	@Override
	public void flush() {
		writeOnce(cursor);
		System.out.println(writeCount);
	}
}
