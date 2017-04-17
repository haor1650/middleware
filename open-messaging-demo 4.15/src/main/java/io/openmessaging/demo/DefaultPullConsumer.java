package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class DefaultPullConsumer implements PullConsumer {
    private MessageStore messageStore = MessageStore.getInstance();
    private KeyValue properties;
    private String queue;
    //由一个队列和多个主题组成的集合
    private Set<String> buckets = new HashSet<>();
    private List<String> bucketList = new ArrayList<>();

    private int lastIndex = 0;

    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
    }


    @Override public KeyValue properties() {
        return properties;
    }

    @Override public Message pull() {

        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public Message pull(KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public Message pull(long timeout, TimeUnit unit) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public Message pull(long timeout, TimeUnit unit, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    //同步方法
    @Override public synchronized Message pullNoWait() {
        if (buckets.size() == 0 || queue == null) {
            return null;
        }
        //use Round Robin
        int checkNum = 0;
        while (++checkNum <= bucketList.size()) {
        	//轮询bucketList的所有元素（包括queue和topic）
            String bucket = bucketList.get((++lastIndex) % (bucketList.size()));
            //根据queue和bucket抽取消息
//            Message message = messageStore.pullMessage(queue, bucket);
//            System.out.println("message:"+message);
            try {
				Message msg = FileObtain.getInstance().obtain(properties.getString("STORE_PATH"), queue, bucket);
//				System.out.println("msg:"+msg);
				return msg;
			} catch (IOException e) {
				e.printStackTrace();
			}
//            if (message != null) {
//                return message;
//            }
        }
        return null;
    }

    @Override public Message pullNoWait(KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public synchronized void attachQueue(String queueName, Collection<String> topics) {
        if (queue != null && !queue.equals(queueName)) {
            throw new ClientOMSException("You have alreadly attached to a queue " + queue);
        }
        //设定该Consumer的消息队列
        queue = queueName;
        //将消息队列加入buckets
        buckets.add(queueName);
        //将topics集合加入buckets
        buckets.addAll(topics);
        bucketList.clear();
        //将所有buckets中的元素加入bucketList
        bucketList.addAll(buckets);
    }

    @Override public void start() {

    }

    @Override public void shutdown() {

    }

}
