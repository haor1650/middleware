package io.openmessaging.demo;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.openmessaging.BytesMessage;
import io.openmessaging.MessageHeader;

/**
 * 利用文件进行消息持久化
 * 采用内存映射文件技术进行优化
 * @author Administrator
 *
 */

public class FileStorageByMMF {
	
	//key：queue/topic，value：映射对象
	private Map<String, MappedByteBuffer> bufferMap = new ConcurrentHashMap<>();
	//private Map<String, BufferedOutputStream> bufferMap = new HashMap<>();
	
	private static FileStorageByMMF instance = new FileStorageByMMF();
	
	private FileStorageByMMF(){}
	
	public static FileStorageByMMF getInstance(){
		return instance;
	}
	
	/**
	 * 将消息持久化到文件
	 * @param storePath
	 * @param bucket
	 * @param message
	 */
	public void store(String storePath, String bucket, BytesMessage message){
		MappedByteBuffer buffer = bufferMap.get(bucket);
		if(buffer == null){
			buffer = createBuffer(storePath, bucket, message);
		}
		buffer.put(message.getBody());
		buffer.put(Constants.NEW_LINE_BREAK);
//		System.out.println("Message:" + message.toString());
	}
	
	/**
	 * 当文件不存在时，创建文件以及输出流
	 * @param storePath
	 * @param bucket
	 * @param message
	 * @return
	 */
	private MappedByteBuffer createBuffer(String storePath,String bucket, BytesMessage message){
		MappedByteBuffer buffer = null;
		try {
            String topic = message.headers().getString(MessageHeader.TOPIC);
            //按照消息类型，区分存储路径
            File file = null;
            if(topic != null){
            	file = new File(storePath+File.separator+Constants.TOPIC_PATH+File.separator+bucket+".txt");
            }else{
            	file = new File(storePath+File.separator+Constants.QUEUE_PATH+File.separator+bucket+".txt");
            }
			if(!file.exists()){
				File path = file.getParentFile();
				if(!path.exists()){
					path.mkdirs();
				}
				file.createNewFile();
			}
			buffer = new RandomAccessFile(file, "rw").getChannel().map(FileChannel.MapMode.READ_WRITE, 0, Constants.MAX_FILE_LENGTH);
//			outputStream = new BufferedOutputStream(new FileOutputStream(file));
			bufferMap.put(bucket, buffer);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return buffer;
	}
	
}
