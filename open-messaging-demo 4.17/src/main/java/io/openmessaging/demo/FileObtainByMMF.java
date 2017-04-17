package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.MessageFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 从文件读取并还原消息对象
 * @author Administrator
 *
 */
public class FileObtainByMMF {
	
	private FileObtainByMMF(){}
	
	private static FileObtainByMMF instance = new FileObtainByMMF();
	
	//key：queue+bucket，value：输入流				//并发问题
	private Map<String, MappedByteBuffer> inputMap = new ConcurrentHashMap<>();
	
	//key：bucket，value：存储的消息类型是否为topic，否则为queue  //并发问题
	private Map<String, Boolean> isTopicMap = new ConcurrentHashMap<>();
	
	private MessageFactory messageFactory = new DefaultMessageFactory();
	
	public static FileObtainByMMF getInstance(){
		return instance;
	}
	
	/**
	 * 从文件中还原消息对象
	 * @param storePath	存储路径
	 * @param queue		调用该方法的消费者绑定的消息队列
	 * @param bucket	消息队列或主题名
	 * @return			
	 * @throws IOException
	 */
	public BytesMessage obtain(String storePath, String queue, String bucket) throws IOException{
		MappedByteBuffer buffer = inputMap.get(queue+bucket);
		if(buffer == null){
			buffer = createInput(storePath, queue, bucket);
		}
		byte[] byteArray = new byte[10000];
		int pos = 0;
		byte temp;
		//TODO此处存在问题，如何判断文件结尾？
		while((temp = buffer.get())!=Constants.NEW_LINE_BREAK[0] && pos < 10000){
			byteArray[pos] = temp;
			pos ++;
		}
		buffer.get();
		
		if(pos >= 10000){
			return null;
		}
		
		BytesMessage message = null;
//		System.out.println("isTopicMap:"+isTopicMap);
		if(isTopicMap.get(bucket)){
			message = messageFactory.createBytesMessageToTopic(bucket, Arrays.copyOf(byteArray, pos));
		}else{
			message = messageFactory.createBytesMessageToQueue(bucket, Arrays.copyOf(byteArray, pos));
		}
		return message;
	}
	
	/**
	 * 新建输入流
	 * @param storePath
	 * @param queue
	 * @param bucket
	 * @return
	 * @throws IOException
	 */
	private MappedByteBuffer createInput(String storePath, String queue, String bucket) throws IOException{
		MappedByteBuffer buffer = null;
		//通过路径判断消息类型 queue或topic
		File file = new File(storePath+File.separator+Constants.QUEUE_PATH+File.separator+bucket+".txt");
		if(!file.exists()){
			isTopicMap.put(bucket, Boolean.TRUE);
			file = new File(storePath+File.separator+Constants.TOPIC_PATH+File.separator+bucket+".txt");
			if(!file.exists()){
				System.out.println("File "+file.getName()+" do not exist");
				throw new IOException();
			}
		}else{
			isTopicMap.put(bucket, Boolean.FALSE);
		}
		buffer = new RandomAccessFile(file, "r").getChannel().map(FileChannel.MapMode.READ_ONLY, 0, Constants.MAX_FILE_LENGTH);
		inputMap.put(queue+bucket, buffer);
		return buffer;
	}
}
