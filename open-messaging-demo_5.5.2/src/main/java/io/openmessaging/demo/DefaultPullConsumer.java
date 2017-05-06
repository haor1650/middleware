package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class DefaultPullConsumer implements PullConsumer {
    private KeyValue properties;
    private String queue;
    //由一个队列和多个主题组成的集合
    private Set<String> buckets = new HashSet<>();
    private List<String> bucketList = new ArrayList<>();
    
    //读取到的消息暂存在消息队列中
    private Queue<DefaultBytesMessage> messageQueue = new ConcurrentLinkedQueue<>();
    //多线程读取各个bucket消息
	private static ThreadPoolExecutor executor = new ThreadPoolExecutor(Constants.CORE_POOL_SIZE, Constants.MAXIMUM_POOL_SIZE, 
							Constants.KEEP_ALIVE_TIME, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(Constants.EXECUTOR_QUEUE_SIZE));
    //TODO 多个consumer可以复用吗 commitlog buffer
    private MappedByteBuffer[] commitLogBuffers;
    //reader对象开启多线程从commitlog读取消息，放入messageQueue队列中
    private MessageReaderByMMF reader;
    
    public int count = 0;// = new AtomicInteger(0);
    
    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
        String storePath = properties.getString("STORE_PATH");
        commitLogBuffers = createBuffers(storePath);
        reader = new MessageReaderByMMF(storePath, messageQueue, commitLogBuffers, executor);
    }

    @Override public KeyValue properties() {
        return properties;
    }

	private static MappedByteBuffer[] createBuffers(String storePath){
		MappedByteBuffer buffer = null;
		File commitLogFileHolder = new File(storePath+File.separator+Constants.COMMITLOG_FOLDER_NAME);
		File[] commitLogFiles = commitLogFileHolder.listFiles();
		MappedByteBuffer[] commitLogBuffers = new MappedByteBuffer[commitLogFiles.length];
		//按文件名称排序
		Arrays.sort(commitLogFiles, new Comparator<File>() {  
			@Override  
			public int compare(File o1, File o2) {  
				if (o1.isDirectory() && o2.isFile())  
					return -1;  
				if (o1.isFile() && o2.isDirectory())  
					return 1;  
				return o1.getName().compareTo(o2.getName());  
			}  
		});
		try {
			for(int i = 0; i < commitLogFiles.length; i ++){
				buffer = MappedByteBufferCreator.createBuffer(commitLogFiles[i], 
						Constants.COMMITLOG_FILE_MAX_LENGTH, false, FileChannel.MapMode.READ_ONLY);
				commitLogBuffers[i] = buffer;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return commitLogBuffers;
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
        		count++;
        		return message;
        	}else{
        		//判断结束
        		if(reader.endCounter.get() == 0){
        			executor.shutdown();
        			return null;
        		}
        		Thread.yield();
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
