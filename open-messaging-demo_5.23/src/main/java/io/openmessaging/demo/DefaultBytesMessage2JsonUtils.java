package io.openmessaging.demo;

import io.openmessaging.KeyValue;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultBytesMessage2JsonUtils {
//	public static String message2JsonString(DefaultBytesMessage message){
//		StringBuilder buf = new StringBuilder();
//		buf.append(message.getBody());
//		return new String(message.getBody());
//	}
	
	/**
	 * 抛弃该策略
	 */
//	public static Map<KeyValue, byte[]> propertiesBytesMap = new ConcurrentHashMap<>();
//	public static Map<KeyValue, byte[]> headersBytesMap = new ConcurrentHashMap<>();
	
//	private static Map<Long, KeyValue> propertiesValueMap = new ConcurrentHashMap<>();
//	private static Map<Long, KeyValue> headersValueMap = new ConcurrentHashMap<>();
	
	
	/**
	 * 5.22 update 添加json参数，用于对象重用，json足够长，返回值为json中有效长度
	 * 转换形式 body + TOKEN + properties + TOKEN + headers
	 * @param message
	 * @return
	 */
	public static byte[] message2JsonByteArray(DefaultBytesMessage message){
		byte[] body = message.getBody();//引用对象
		byte[] properties = null;		//引用对象
		KeyValue propertiesKeyValue = message.getProperties();
		if(propertiesKeyValue != null){
			properties = message.properties().toString().getBytes();//TODO 耗时操作
		}
		int propertiesLen = properties == null ? 0 : properties.length;
		
		byte[] headers = null;			//引用对象
		KeyValue headersKeyValue = message.getHeaders();
		if(headersKeyValue != null){
			headers = message.headers().toString().getBytes();//TODO 耗时操作
		}
		int headersLen = headers == null ? 0 : headers.length;
		
		int tokenLen = Constants.MESSAGE_JSON_TOKEN.length;
		byte[] json = new byte[body.length + tokenLen + propertiesLen + tokenLen + headersLen];
		int destPos = 0;
		System.arraycopy(body, 0, json, 0, body.length);
		destPos += body.length;
		System.arraycopy(Constants.MESSAGE_JSON_TOKEN, 0, json, destPos, tokenLen);
		destPos += tokenLen;
		if(properties != null){
			System.arraycopy(properties, 0, json, destPos, properties.length);
		}
		destPos += propertiesLen;
		System.arraycopy(Constants.MESSAGE_JSON_TOKEN, 0, json, destPos, tokenLen);
		destPos += tokenLen;
		if(headers != null){
			System.arraycopy(headers, 0, json, body.length + tokenLen + propertiesLen + tokenLen, headers.length);
		}
		return json;
	}
	
	public static int message2JsonByteArray(DefaultBytesMessage message, byte[] json){
		byte[] body = message.getBody();//引用对象
		byte[] properties = null;		//引用对象
		KeyValue propertiesKeyValue = message.getProperties();
		if(propertiesKeyValue != null){
//			properties = propertiesBytesMap.get(propertiesKeyValue);
//			if(properties == null){
				properties = message.properties().toString().getBytes();//TODO 耗时操作
//				propertiesBytesMap.put(propertiesKeyValue, properties);
//			}
		}
		int propertiesLen = properties == null ? 0 : properties.length;
		
		byte[] headers = null;			//引用对象
		KeyValue headersKeyValue = message.getHeaders();
		if(headersKeyValue != null){
//			headers = headersBytesMap.get(headersKeyValue);
//			if(headers == null){
				headers = message.headers().toString().getBytes();//TODO 耗时操作
//				headersBytesMap.put(headersKeyValue, headers);
//			}
		}
		int headersLen = headers == null ? 0 : headers.length;
		
		int tokenLen = Constants.MESSAGE_JSON_TOKEN.length;
//		byte[] json = new byte[body.length + tokenLen + propertiesLen + tokenLen + headersLen];
		int usefulLen = body.length + tokenLen + propertiesLen + tokenLen + headersLen;
		int destPos = 0;
		System.arraycopy(body, 0, json, 0, body.length);
		destPos += body.length;
		System.arraycopy(Constants.MESSAGE_JSON_TOKEN, 0, json, destPos, tokenLen);
		destPos += tokenLen;
		if(properties != null){
			System.arraycopy(properties, 0, json, destPos, properties.length);
		}
		destPos += propertiesLen;
		System.arraycopy(Constants.MESSAGE_JSON_TOKEN, 0, json, destPos, tokenLen);
		destPos += tokenLen;
		if(headers != null){
			System.arraycopy(headers, 0, json, body.length + tokenLen + propertiesLen + tokenLen, headers.length);
		}
		return usefulLen;
	}
	
	public static DefaultBytesMessage byteArray2Message(byte[] byteArray){
		DefaultBytesMessage message = null;
		int start = 0;
		int index = 0; //记录解析的步骤 0-body，1-properties
		for(int i = 0 ; i < byteArray.length ; i ++){
			byte b = byteArray[i];
			//TODO 仅适用一个字符进行分割是否可行？
			if(b == Constants.MESSAGE_JSON_TOKEN[0]){
				if(index == 0){//body
					message = createMsg(Arrays.copyOfRange(byteArray, 0, i));
					start = i + 1;
					index ++;
				}else if(index == 1){//properties
					message.setProperties(bytes2KeyValue(byteArray, start, i, false));
					start = i + 1;
					break;
				}
			}
		}
		message.setHeaders(bytes2KeyValue(byteArray, start, byteArray.length, true));
		return message;
	}
	
	public static DefaultBytesMessage createMsg(byte[] body){
		return new DefaultBytesMessage(body);
	}
	
	public static KeyValue bytes2KeyValue(byte[] byteArray, int start, int end, boolean isHeaders){
		if(start == end){
			return null;
		}
		Long byteArrayHashCode = byteArrayHashCode(byteArray, start, end);
		
		/**
		 * 抛弃策略
		 */
//		if(isHeaders){
//			KeyValue headers = headersValueMap.get(byteArrayHashCode);
//			if(headers != null){
//				return headers;
//			}
//		}else{
//			KeyValue properties = propertiesValueMap.get(byteArrayHashCode);
//			if(properties != null){
//				return properties;
//			}
//		}
		
//		///////由于将出现过的KeyValue保存在map中，以下代码仅在很少情况调用
		
		KeyValue kv = new DefaultKeyValue();
		int keyValueEntryStart = start;
		int keyValueBreak = 0;
		for(int i = start ; i < end; i ++){
			byte c = byteArray[i];
			if(c == '='){
				keyValueBreak = i;
			}else if(c == ','){
				kv.put(new String(byteArray, keyValueEntryStart, keyValueBreak - keyValueEntryStart),
						new String(byteArray, keyValueBreak + 1, i - (keyValueBreak + 1)));
				keyValueEntryStart = i + 1;
			}
		}
		try {
			kv.put(new String(byteArray, keyValueEntryStart, keyValueBreak - keyValueEntryStart),
					new String(byteArray, keyValueBreak + 1, end - (keyValueBreak + 1)));
		} catch (RuntimeException e) {
			e.printStackTrace();
			System.out.println("keyValueEntryStart "+keyValueEntryStart+", keyValueBreak "+ keyValueBreak);
			System.out.println(new String(byteArray));
			throw e;
		}
		
//		if(isHeaders){
//			headersValueMap.put(byteArrayHashCode, kv);
//		}else{
//			propertiesValueMap.put(byteArrayHashCode, kv);
//		}
		
		return kv;
		
	}
	
	private static Long byteArrayHashCode(byte[] byteArray, int start, int end){
        long hash = 0;  
        int count = start;  
        while (count < end) {
          hash = byteArray[count] + (hash << 7) + (hash << 16) - hash;
          count++;
         }
        return hash; 
	}
	
}
