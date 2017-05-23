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
	public static final long COMMITLOG_FILE_MAX_LENGTH = 1024*1024*1024;
	//comiitlog文件最大可写长度，当超出该长度时，申请新的commitlog文件
	public static final long COMMITLOG_FILE_MAX_WRITE_LENGTH = 1024*1024*1023;
	//commitqueue文件夹名
	public static final String COMMITQUEUE_FOLDER_NAME = "commitqueue";
	//commitqueue文件最大长度
	public static final long COMMITQUEUE_FILE_MAX_LENGTH = 1024*1024*10;	//TODO test
	
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
	public static final int CORE_POOL_SIZE = 4;	//5.4修改
	public static final int MAXIMUM_POOL_SIZE = 4;
	public static final int KEEP_ALIVE_TIME = 100;	//ms
	public static final int EXECUTOR_QUEUE_SIZE = 40;
	
	//commit log thread 等待消息时间
	public static final int WAIT_FOR_MESSAGE_TIME = 50;//ms
	
	/**
	 * 5.2添加
	 */
	public static final int CONSUMER_WAIT_FOR_MESSAGE_TIME = 10;//ms
	
	/**
	 * 5.3添加
	 */
	public static final int LENGH_BYTES = 20; // length在offsetAndLength中所占后几位
	public static final int LENGH_PLACEHOLDER = 0xFFFFF; //length的占位符，offsetAndLength进行与操作
	/**
	 * 5.4添加
	 */
	public static final int COMMIT_LOG_FILE_OFFSET_BYTES = 30; 	//offset中在文件中的位置所占位数，与COMMITLOG_FILE_MAX_LENGTH对应
	public static final int COMMIT_LOG_FILE_OFFSET_PLACEHOLDER = 0x3FFFFFFF;//offset取与操作，得到单个文件内的offset
	/**
	 * 5.5添加
	 */
	public static final int COMMIT_QUEUE_READ_DOZEN = 10000; //读取commitqueue，读取多个long后就进行readcommitlog
	/**
	 * 5.6添加
	 */
	public static final int WORKING_PRODUCER_NUM_LIMIT = 1;	//TODO test同时允许工作的生产者数量
	/**
	 * 5.8添加
	 */
	public static final int MSG_BLOCKING_QUEUE_LENGTH = 100;
	public static final int ONCE_WRITE_DOZEN = 700;		//TODO 一个生产者一次写入的消息条数 500-1000
	
	/**
	 * 5.9添加
	 */
	public static final int UPDATE_COMMIT_QUEUE_BUFFER_LENGTH = COMMIT_QUEUE_READ_DOZEN*8*100;	//commitqueue 一次更新使用的长度

	/**
	 * 5.22添加
	 */
	public static final int JSON_MAX_LENGTH = 1024*110;	//json数组最大长度
}
