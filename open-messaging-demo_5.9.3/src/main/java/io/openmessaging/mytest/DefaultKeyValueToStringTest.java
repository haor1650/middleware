package io.openmessaging.mytest;

import io.openmessaging.demo.DefaultKeyValue;

public class DefaultKeyValueToStringTest {

	public static void main(String[] args) {
		DefaultKeyValue kvs = new DefaultKeyValue();
		kvs.put("queue", "queue1");
		kvs.put("topic", "qwe");
		System.out.println(kvs);
	}
	
}
