package io.openmessaging.mytest;

import java.util.HashMap;

public class PrintHashMapTest {
	
	public static void main(String[] args) {
		HashMap<String, Object> hashMap = new HashMap<String, Object>();
		hashMap.put("a", 1);
		hashMap.put("b", 1.0);
		hashMap.put("c", "ok");
		System.out.println(hashMap);
	}
}
