package io.openmessaging.demo;

/**
 * 维护一个消息JSON，并记录bucket
 * @author Administrator
 *
 */
public class BytesMessageJson {
	private byte[] json;
	private String bucket;
	
	public BytesMessageJson(byte[] json, String bucket) {
		super();
		this.json = json;
		this.bucket = bucket;
	}
	public byte[] getJson() {
		return json;
	}
	public void setJson(byte[] json) {
		this.json = json;
	}
	public String getBucket() {
		return bucket;
	}
	public void setBucket(String bucket) {
		this.bucket = bucket;
	}
	
}
