package io.openmessaging.mytest;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class CommitQueueFileTest {
	public static void main(String[] args) {
		try {
			File parentFile = new File("temp\\commitqueue");
			File[] listFiles = parentFile.listFiles();
			RandomAccessFile rand = null;
			long tmpLong = 0;
			int count = 0;
			
			for(File file: listFiles){
				int tem = 0;
				rand = new RandomAccessFile(file, "r");
				while(true){
					tmpLong = rand.readLong();
					if(tmpLong == 0){
						break;
					}
					tem ++;
				}
				System.out.println(file.getName()+":"+tem);
				count += tem;
			}
			System.out.println(count);
			
//			RandomAccessFile file = new RandomAccessFile("temp\\commitqueue\\TOPIC_0_offset.txt", "r");
//			RandomAccessFile file = new RandomAccessFile("1493782724041\\commitqueue\\queue1_offset.txt", "r");
//			long tmpLong;
//			int count = 0;
//			while(true){
//				tmpLong = file.readLong();
//				System.out.println("Offset:"+(tmpLong >> 20)+", Length:" + (tmpLong & 0xFFFFF) + ","+ count);
//				if(count ++ > 10000){
//					break;
//				}
//			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
