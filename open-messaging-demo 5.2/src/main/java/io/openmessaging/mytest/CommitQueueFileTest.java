package io.openmessaging.mytest;

import java.io.IOException;
import java.io.RandomAccessFile;

public class CommitQueueFileTest {
	public static void main(String[] args) {
		try {
			RandomAccessFile file = new RandomAccessFile("temp\\commitqueue\\QUEUE_9_offset.txt", "r");
			int tmpLong;
			int tmpInt;
			int count = 0;
			while(true){
				tmpLong = file.readInt();
				tmpInt = file.readInt();
				System.out.println("Long:"+tmpLong+", Int" + tmpInt);
				if(count ++ > 10090){
					break;
				}
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
