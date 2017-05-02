package io.openmessaging.demo;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 顺序记录同一个bucket下的所有消息的offset，保存在一个commitqueue中
 * @author Administrator
 *
 */
public class CommitQueueFileStorageByMMF {
	//内容映射文件对象
	private MappedByteBuffer buffer;
	
	private String storePath;
	
	public CommitQueueFileStorageByMMF(String storePath){
		this.storePath = storePath;
	}
	
	/**
	 * 将offset信息写入文件
	 * @param offset
	 */
	public void storeCommitQueue(BytesMessageOffset offset){
		if(buffer == null){
			buffer = createCommitQueueBuffer(offset.getBucket());
		}
		buffer.put(BytesTransferUtils.int2Bytes(offset.getOffset()));
		buffer.put(BytesTransferUtils.int2Bytes(offset.getLength()));
	}
	
	/**
	 * 新建commitqueue文件并返回buffer对象
	 * @param storePath
	 * @return
	 */
	private MappedByteBuffer createCommitQueueBuffer(String bucket){
		MappedByteBuffer buffer = null;
		try {
			String fullPathName = storePath + File.separator + Constants.COMMITQUEUE_FOLDER_NAME
					+ File.separator  + bucket + "_offset.txt";
			buffer = MappedByteBufferCreator.createBuffer(fullPathName, Constants.COMMITQUEUE_FILE_MAX_LENGTH, true, FileChannel.MapMode.READ_WRITE);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return buffer;
	}
	
	public void closeBuffer(){
		try {
			  Method getCleanerMethod = buffer.getClass().getMethod("cleaner", new Class[0]);  
			  getCleanerMethod.setAccessible(true);
			  sun.misc.Cleaner cleaner = (sun.misc.Cleaner)getCleanerMethod.invoke(buffer,new Object[0]);
			  cleaner.clean();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
