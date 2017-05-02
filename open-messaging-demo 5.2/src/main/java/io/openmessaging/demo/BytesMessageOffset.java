package io.openmessaging.demo;

/**
 * 维护一个消息在commilog文件中的offset和length
 * @author Administrator
 */
//TODO 尝试用多个队列替换该类
public class BytesMessageOffset {
	private int offset;
	private int length;
	private String bucket;
	public BytesMessageOffset(int offset, int length, String bucket) {
		super();
		this.offset = offset;
		this.length = length;
		this.bucket = bucket;
	}
	public int getOffset() {
		return offset;
	}
	public int getLength() {
		return length;
	}
	public String getBucket() {
		return bucket;
	}
	@Override
	public String toString() {
		return "BytesMessageOffset [offset=" + offset + ", length=" + length
				+ ", bucket=" + bucket + "]";
	}
}
