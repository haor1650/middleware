package io.openmessaging.demo;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

/**
 * 创建文件映射buffer对象工具类
 * @author Administrator
 *
 */

public class MappedByteBufferCreator {
	
	/**
	 * 创建buffer
	 * @param fullPathName
	 * @param maxLength
	 * @param createIfNotExist
	 * @param mode
	 * @return
	 * @throws IOException 
	 */
	public static MappedByteBuffer createBuffer(String fullPathName, long position, long maxLength, boolean createIfNotExist, MapMode mode) throws IOException{
		MappedByteBuffer buffer = null;
		try {
			File file = new File(fullPathName);
			buffer = createBuffer(file, position, maxLength, createIfNotExist, mode);
		} catch (IOException e) {
			e.printStackTrace();
			throw e;
		}
		return buffer;
	}
	
	public static MappedByteBuffer createBuffer(File commitLogFile, long position, long maxLength, boolean createIfNotExist, MapMode mode) throws IOException{
		MappedByteBuffer buffer = null;
		try {
			File file = commitLogFile;
			if(!file.exists()){
				if(!createIfNotExist){
					throw new RuntimeException("文件："+file.getName()+"未能找到");
				}else{
					File path = file.getParentFile();
					if(!path.exists()){
						path.mkdirs();
					}
					file.createNewFile();
				}
			}
			RandomAccessFile randAccessFile = null;
			if(mode == FileChannel.MapMode.READ_ONLY){
				randAccessFile = new RandomAccessFile(file, "r");
				buffer = randAccessFile.getChannel().map(mode, position, maxLength);
			}else if(mode == FileChannel.MapMode.READ_WRITE){
				randAccessFile = new RandomAccessFile(file, "rw");
				buffer = randAccessFile.getChannel().map(mode, position, maxLength);
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw e;
		}
		return buffer;
	}
	
	public static MappedByteBuffer createCommitLogBuffer(String storePath, int commitLogIndex){
		MappedByteBuffer buffer = null;
		try {
			String pathFullName = storePath + File.separator + Constants.COMMITLOG_FOLDER_NAME + 
					File.separator + Constants.COMMITLOG_FILE_NAME + commitLogIndex + ".txt";
			buffer = MappedByteBufferCreator.createBuffer(pathFullName, 0, Constants.COMMITLOG_FILE_MAX_LENGTH, true, FileChannel.MapMode.READ_WRITE);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return buffer;
	}
	
	/**
	 * 给定storePath, 返回路径下的所有commitlog文件buffer
	 * @param storePath
	 * @return
	 */
	public static MappedByteBuffer[] createCommitLogBuffers(String storePath){
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
				buffer = MappedByteBufferCreator.createBuffer(commitLogFiles[i], 0, 
						Constants.COMMITLOG_FILE_MAX_LENGTH, false, FileChannel.MapMode.READ_ONLY);
				commitLogBuffers[i] = buffer;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return commitLogBuffers;
	}
	
	public static MappedByteBuffer createCommitQueueBuffer(String storePath, String bucket, long position, long size){
		MappedByteBuffer buffer = null;
		try {
			String fullPathName = storePath + File.separator + Constants.COMMITQUEUE_FOLDER_NAME + 
					File.separator + bucket + "_offset.txt";
			buffer = createBuffer(fullPathName, position, size, false, FileChannel.MapMode.READ_ONLY);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return buffer;
	}
	
}
