package io.openmessaging.mytest;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Comparator;
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
		String storePath = "temp";
		Queue<DefaultBytesMessage> messageQueue = new ConcurrentLinkedQueue<>();
		ThreadPoolExecutor executor = new ThreadPoolExecutor(2, 4, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
		MappedByteBuffer[] commitLogBuffers = createBuffers(storePath);
		MessageReaderByMMF reader = new MessageReaderByMMF(storePath, 
				messageQueue, commitLogBuffers, executor);
		try {
			reader.registBucket("QUEUE_0");
//			reader.registBucket("QUEUE_1");
			Thread.sleep(1000);
			for(DefaultBytesMessage msg : messageQueue){
				System.out.println(msg);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
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

	
}
