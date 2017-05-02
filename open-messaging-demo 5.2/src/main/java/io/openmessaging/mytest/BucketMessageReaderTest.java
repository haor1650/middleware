package io.openmessaging.mytest;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import io.openmessaging.demo.Constants;
import io.openmessaging.demo.DefaultBytesMessage;
import io.openmessaging.demo.MappedByteBufferCreator;
import io.openmessaging.demo.MessageReaderByMMF;

public class BucketMessageReaderTest {
	public static void main(String[] args) {
		String storePath = "1493115580936";
		Queue<DefaultBytesMessage> messageQueue = new ConcurrentLinkedQueue<>();
		ThreadPoolExecutor executor = new ThreadPoolExecutor(2, 4, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
		MappedByteBuffer commitLogBuffer = createBuffer(storePath);
		MessageReaderByMMF reader = new MessageReaderByMMF(storePath, 
				messageQueue, commitLogBuffer, executor);
		try {
			reader.registBucket("queue1");
			reader.registBucket("queue2");
			Thread.sleep(1000);
			for(DefaultBytesMessage msg : messageQueue){
				System.out.println(msg);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static MappedByteBuffer createBuffer(String storePath){
		MappedByteBuffer buffer = null;
		try {
			buffer = MappedByteBufferCreator.createBuffer(storePath+File.separator+Constants.COMMITLOG_FOLDER_NAME+File.separator+"commitlog1.txt", 
					1024*1024*2, false, FileChannel.MapMode.READ_ONLY);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return buffer;
	}
	
}
