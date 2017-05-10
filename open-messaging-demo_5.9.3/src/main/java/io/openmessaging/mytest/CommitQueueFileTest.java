package io.openmessaging.mytest;

import java.io.IOException;
import java.io.RandomAccessFile;

public class CommitQueueFileTest {
	public static void main(String[] args) {
		try {
			RandomAccessFile file = new RandomAccessFile("temp\\commitqueue\\TOPIC_0_offset.txt", "r");
//			RandomAccessFile file = new RandomAccessFile("1493782724041\\commitqueue\\queue1_offset.txt", "r");
			long tmpLong;
			int count = 0;
			while(true){
				tmpLong = file.readLong();
				System.out.println("Offset:"+(tmpLong >> 20)+", Length:" + (tmpLong & 0xFFFFF) + ","+ count);
				if(count ++ > 10000){
					break;
				}
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
