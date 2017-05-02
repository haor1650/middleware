package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultPullConsumer implements PullConsumer {
    private KeyValue properties;
    private String queue;
    //由一个队列和多个主题组成的集合
    private Set<String> buckets = new HashSet<>();
    private List<String> bucketList = new ArrayList<>();
    
    //读取到的消息暂存在消息队列中
    private Queue<DefaultBytesMessage> messageQueue = new ConcurrentLinkedQueue<>();
    //多线程读取各个bucket消息
	private static ThreadPoolExecutor executor = new ThreadPoolExecutor(Constants.CORE_POOL_SIZE, Constants.MAX_FILE_LENGTH, 
							Constants.KEEP_ALIVE_TIME, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(Constants.EXECUTOR_QUEUE_SIZE));
    //commitlog buffer
    private MappedByteBuffer commitLogBuffer;
    //reader对象开启多线程从commitlog读取消息，放入messageQueue队列中
    private MessageReaderByMMF reader;
//    private int lastIndex = 0;
    
//    private boolean isStart = false;
    
    public int count = 0;// = new AtomicInteger(0);
    //当消息队列为空messageQueue时进入等待，超时后说明消费完毕
    private int waitForMsgCount = 0;
    
    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
        String storePath = properties.getString("STORE_PATH");
        commitLogBuffer = createBuffer(storePath);
        reader = new MessageReaderByMMF(storePath, messageQueue, commitLogBuffer, executor);
    }

    @Override public KeyValue properties() {
        return properties;
    }

	public static MappedByteBuffer createBuffer(String storePath){
		MappedByteBuffer buffer = null;
		try {
			buffer = MappedByteBufferCreator.createBuffer(storePath+File.separator+Constants.COMMITLOG_FOLDER_NAME+File.separator+"commitlog1.txt", 
					Constants.COMMITLOG_FILE_MAX_LENGTH, false, FileChannel.MapMode.READ_ONLY);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return buffer;
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
        
        //注册所有bucket，开始读取和解析
        reader.registBucket(buckets);
    }


	@Override
	public Message poll() {
        if (buckets.size() == 0 || queue == null) {
            return null;
        }
        while(true){
        	DefaultBytesMessage message = messageQueue.poll();
        	if(message != null){
//        		isStart = true;
        		count++;
        		waitForMsgCount = 0;
        		return message;
        	}else{
        		//判断结束
        		if(reader.endCounter.get() == 0){
        			executor.shutdown();
        			return null;
        		}
        		Thread.yield();
        		
//				try {
//					Thread.sleep(1);
//					if(waitForMsgCount ++ > Constants.CONSUMER_WAIT_FOR_MESSAGE_TIME){//判断消费是否完成
//						System.out.println(Thread.currentThread().getName()+"Commit Log Produce "+ count);
//						return null;
//					}
//				} catch (InterruptedException e) {
//					e.printStackTrace();
//				}

//        		if(isStart == true){
//        			return null;
//        		}
        	}
        }
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
