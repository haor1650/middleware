package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.Message;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class MessageStore {

    private static final MessageStore INSTANCE = new MessageStore();

    public static MessageStore getInstance() {
        return INSTANCE;
    }

    //key：Bucket		value：message list
    private Map<String, ArrayList<Message>> messageBuckets = new HashMap<>();

    //key：queue		value：map<key：Bucket，value：offset>，对每个queue维护一个offsetmap
    private Map<String, HashMap<String, Integer>> queueOffsets = new HashMap<>();

    //对queue和topic的操作的是相同的
    public synchronized void putMessage(String bucket, Message message) {
        if (!messageBuckets.containsKey(bucket)) {
            messageBuckets.put(bucket, new ArrayList<>(1024));
        }
        ArrayList<Message> bucketList = messageBuckets.get(bucket);
        bucketList.add(message);
        
    }

   public synchronized Message pullMessage(String queue, String bucket) {
        ArrayList<Message> bucketList = messageBuckets.get(bucket);
        if (bucketList == null) {
            return null;
        }
        HashMap<String, Integer> offsetMap = queueOffsets.get(queue);
        if (offsetMap == null) {
            offsetMap = new HashMap<>();
            queueOffsets.put(queue, offsetMap);
        }
        int offset = offsetMap.getOrDefault(bucket, 0);
        if (offset >= bucketList.size()) {
            return null;
        }
        Message message = bucketList.get(offset);
        offsetMap.put(bucket, ++offset);
        return message;
   }
}
