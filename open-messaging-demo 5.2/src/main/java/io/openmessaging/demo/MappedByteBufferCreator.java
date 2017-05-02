package io.openmessaging.demo;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

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
	public static MappedByteBuffer createBuffer(String fullPathName, long maxLength, boolean createIfNotExist, MapMode mode) throws IOException{
		MappedByteBuffer buffer = null;
		try {
			File file = new File(fullPathName);
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
			if(mode == FileChannel.MapMode.READ_ONLY){
				buffer = new RandomAccessFile(file, "r").getChannel().map(mode, 0, maxLength);
			}else if(mode == FileChannel.MapMode.READ_WRITE){
				buffer = new RandomAccessFile(file, "rw").getChannel().map(mode, 0, maxLength);
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw e;
		}
		return buffer;
	}
	
}
