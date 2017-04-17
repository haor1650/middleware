package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.MessageFactory;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 从文件读取并还原消息对象
 * @author Administrator
 *
 */
public class FileObtain {
	
	private FileObtain(){}
	
	private static FileObtain instance = new FileObtain();
	
	//key：queue+bucket，value：输入流				//并发问题
	private Map<String, BufferedReader> inputMap = new ConcurrentHashMap<>();
	
	//key：bucket，value：存储的消息类型是否为topic，否则为queue  //并发问题
	private Map<String, Boolean> isTopicMap = new ConcurrentHashMap<>();
	
	private MessageFactory messageFactory = new DefaultMessageFactory();
	
	public static FileObtain getInstance(){
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
		BufferedReader reader = inputMap.get(queue+bucket);
		if(reader == null){
			reader = createInput(storePath, queue, bucket);
		}
		String messageLine = reader.readLine();
		//TODO 如何判断文件结束？
		if(messageLine == null || messageLine.length() > 10000){
			return null;
		}
		BytesMessage message = null;
//		System.out.println("isTopicMap:"+isTopicMap);
		if(isTopicMap.get(bucket)){
			message = messageFactory.createBytesMessageToTopic(bucket, messageLine.getBytes());
		}else{
			message = messageFactory.createBytesMessageToQueue(bucket, messageLine.getBytes());
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
	private BufferedReader createInput(String storePath, String queue, String bucket) throws IOException{
		BufferedReader reader = null;
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
		reader = new BufferedReader(new FileReader(file));
		inputMap.put(queue+bucket, reader);
		return reader;
	}
}
