package io.openmessaging.demo;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import io.openmessaging.BytesMessage;
import io.openmessaging.MessageHeader;

/**
 * 利用文件进行消息持久化
 * @author Administrator
 *
 */

public class FileStorageWriter {
	
	public static final byte[] NEW_LINE = {'\r','\n'};
	
	public static final String QUEUE_PATH = "QUEUE";
	
	public static final String TOPIC_PATH = "TOPIC";
	
	//key：queue/topic，value：输出流				需解决并发问题
	private Map<String, PrintWriter> outputMap = new HashMap<>();
	
	private static FileStorageWriter instance = new FileStorageWriter();
	
	private FileStorageWriter(){}
	
	public static FileStorageWriter getInstance(){
		return instance;
	}
	
	/**
	 * 将消息持久化到文件
	 * @param storePath
	 * @param bucket
	 * @param message
	 */
	public void store(String storePath, String bucket, BytesMessage message){
		PrintWriter outputStream = outputMap.get(bucket);
		if(outputStream == null){
			outputStream = createOutputStream(storePath, bucket, message);
		}
//		try {
			outputStream.println(new String(message.getBody()));
//			outputStream.write(NEW_LINE);
			outputStream.flush();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
	}
	
	/**
	 * 当文件不存在时，创建文件以及输出流
	 * @param storePath
	 * @param bucket
	 * @return
	 */
	private PrintWriter createOutputStream(String storePath,String bucket, BytesMessage message){
		PrintWriter outputStream = null;
		try {
            String topic = message.headers().getString(MessageHeader.TOPIC);
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
			outputStream = new PrintWriter(new FileOutputStream(file));
			outputMap.put(bucket, outputStream);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return outputStream;
	}
	
}
