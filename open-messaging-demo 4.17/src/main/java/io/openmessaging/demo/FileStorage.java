package io.openmessaging.demo;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import io.openmessaging.BytesMessage;
import io.openmessaging.MessageHeader;

/**
 * 利用文件进行消息持久化
 * @author Administrator
 *
 */

public class FileStorage {
	
	public static final byte[] NEW_LINE = {'\r','\n'};
	
	public static final String QUEUE_PATH = "QUEUE";
	
	public static final String TOPIC_PATH = "TOPIC";
	
	//key：queue/topic，value：输出流				需解决并发问题
	private Map<String, BufferedOutputStream> outputMap = new HashMap<>();
	
	private static FileStorage instance = new FileStorage();
	
	private FileStorage(){}
	
	public static FileStorage getInstance(){
		return instance;
	}
	
	/**
	 * 将消息持久化到文件
	 * @param storePath
	 * @param bucket
	 * @param message
	 */
	public void store(String storePath, String bucket, BytesMessage message){
		BufferedOutputStream outputStream = outputMap.get(bucket);
		if(outputStream == null){
			outputStream = createOutputStream(storePath, bucket, message);
		}
		try {
			outputStream.write(message.getBody());
			outputStream.write(NEW_LINE);
			outputStream.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 当文件不存在时，创建文件以及输出流
	 * @param storePath
	 * @param bucket
	 * @param message
	 * @return
	 */
	private BufferedOutputStream createOutputStream(String storePath,String bucket, BytesMessage message){
		BufferedOutputStream outputStream = null;
		try {
            String topic = message.headers().getString(MessageHeader.TOPIC);
            //按照消息类型，区分存储路径
            File file = null;
            if(topic != null){
            	file = new File(storePath+File.separator+TOPIC_PATH+File.separator+bucket+".txt");
            }else{
            	file = new File(storePath+File.separator+QUEUE_PATH+File.separator+bucket+".txt");
            }
			if(!file.exists()){
				File path = file.getParentFile();
				if(!path.exists()){
					path.mkdirs();
				}
				file.createNewFile();
			}
			outputStream = new BufferedOutputStream(new FileOutputStream(file));
			outputMap.put(bucket, outputStream);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return outputStream;
	}
	
}
