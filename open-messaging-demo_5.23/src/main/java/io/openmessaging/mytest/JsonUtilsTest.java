package io.openmessaging.mytest;

import io.openmessaging.KeyValue;
import io.openmessaging.demo.DefaultBytesMessage;
import io.openmessaging.demo.DefaultBytesMessage2JsonUtils;

public class JsonUtilsTest {

	public static void main(String[] args) {
		byte[] body = "hello json".getBytes();
		DefaultBytesMessage msg = new DefaultBytesMessage(body);
		msg.putHeaders("topic", "topic123");
		msg.putHeaders("queue", "queue123");
//		msg.putProperties("storePath", "path");
//		System.out.println(msg);
		long start = System.currentTimeMillis();
//		byte[] jsonByteArray = DefaultBytesMessage2JsonUtils.message2JsonByteArray(msg);
		DefaultBytesMessage byteArray2Message = null;
//		for(int i = 0; i < 2000000; i ++){
//			jsonByteArray = DefaultBytesMessage2JsonUtils.message2JsonByteArray(msg);				//耗时36% 
//			byteArray2Message = DefaultBytesMessage2JsonUtils.byteArray2Message(jsonByteArray);		//耗时65%
//		}
		
			
		long end = System.currentTimeMillis();
		System.out.println(""+(end - start));
	}
	
}
