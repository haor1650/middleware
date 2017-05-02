package io.openmessaging.demo;

public class Constants {
	
	/**
	 * 4.17版
	 */
	//换行符
	public static final byte[] NEW_LINE_BREAK = {'\r','\n'};
	//队列路径
	public static final String QUEUE_PATH = "QUEUE";
	//主题路径
	public static final String TOPIC_PATH = "TOPIC";
	//文件最大长度
	public static final int MAX_FILE_LENGTH = 1024*1024*2;
	
	/**
	 * 4.24版
	 */
	//commitLog文件名
	public static final String COMMITLOG_FILE_NAME = "commitlog";
	//commitlog文件夹名
	public static final String COMMITLOG_FOLDER_NAME = "commitlog";
	//commitlog文件最大长度
	public static final long COMMITLOG_FILE_MAX_LENGTH = 1024*1024*300;
	//commitqueue文件夹名
	public static final String COMMITQUEUE_FOLDER_NAME = "commitqueue";
	//commitqueue文件最大长度
	public static final long COMMITQUEUE_FILE_MAX_LENGTH = 1024*1024*100;	//TODO test
	
	/**
	 * 4.26添加
	 */
	//自定义JSON中body与header，header与properties之间的连接符
	public static final byte[] MESSAGE_JSON_TOKEN = {'\n'};
	public static final String MESSAGE_JSON_TOKEN_STR = "\n";
	
	/**
	 * 4.27添加
	 */
	//线程池参数
	public static final int CORE_POOL_SIZE = 10;	//5.2修改
	public static final int MAXIMUM_POOL_SIZE = 20;
	public static final int KEEP_ALIVE_TIME = 100;	//ms
	public static final int EXECUTOR_QUEUE_SIZE = 20;
	
	//commit log thread 等待消息时间
	public static final int WAIT_FOR_MESSAGE_TIME = 5;//ms
	
	/**
	 * 5.2添加
	 */
	public static final int CONSUMER_WAIT_FOR_MESSAGE_TIME = 10;//ms
	
	
}
