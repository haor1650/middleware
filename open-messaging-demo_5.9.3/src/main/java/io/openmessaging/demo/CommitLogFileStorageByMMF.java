package io.openmessaging.demo;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 在单个线程中记录commitlog文件
 * 采用内存映射文件技术进行优化
 * @author Administrator
 */

public class CommitLogFileStorageByMMF {
	
	//内容映射文件对象
	private MappedByteBuffer buffer;
	
	//每次记录文件后，offset指向文件末尾的下一个位置
	private long offset;
	
	//当前已经记录了多少个commitlog文件
	private int commitlogFileIndex = 0;
	
	private String storePath;
	//需要新建文件时的长度
	private long needNewLogFileLength = Constants.COMMITLOG_FILE_MAX_WRITE_LENGTH;
	
	public CommitLogFileStorageByMMF(String storePath){
		this.storePath = storePath;
	}
	
	/**
	 * 记录commitlog文件，返回记录所在文件中的位置
	 * @param storePath
	 * @param messageJson
	 * @return
	 */
	public synchronized long storeCommitLog(byte[] messageJson){
		long recordOffset = offset;
		if(buffer == null){
			buffer = createCommitLogBuffer(/*String storePath,*/);
		}
		buffer.put(messageJson);
		offset += messageJson.length;
		//当将要超出文件大小时，申请新文件
		if(offset > needNewLogFileLength){
			closeBuffer();
			buffer = createCommitLogBuffer();
			needNewLogFileLength += Constants.COMMITLOG_FILE_MAX_LENGTH;
			System.gc();
			offset = Constants.COMMITLOG_FILE_MAX_LENGTH * (commitlogFileIndex - 1);
		}
		return recordOffset;
	}
	
	/**
	 * 新建commitLog文件并返回buffer对象
	 * @param storePath
	 * @return
	 */
	private synchronized MappedByteBuffer createCommitLogBuffer(/*String storePath,*/){
		MappedByteBuffer buffer = null;
		try {
			commitlogFileIndex ++;
			String pathFullName = storePath + File.separator + Constants.COMMITLOG_FOLDER_NAME + 
					File.separator + Constants.COMMITLOG_FILE_NAME + commitlogFileIndex + ".txt";
			buffer = MappedByteBufferCreator.createBuffer(pathFullName, 0, Constants.COMMITLOG_FILE_MAX_LENGTH, true, FileChannel.MapMode.READ_WRITE);
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
