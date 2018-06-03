package com.utils.kafka.simpleconsumer;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.*;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

public class KafkaConsumerManager {
    private static Logger log = LoggerFactory.getLogger(KafkaConsumerManager.class);
    //装当前分区及其备份的设备的host
    private List<String> m_replicaBrokers ;

    public KafkaConsumerManager() {
        this.m_replicaBrokers = new ArrayList<String>();
    }

    /**
     * 消费
     * @param a_maxReads    本次获取最大的数据量。可以理解为一次返回多少条数据
     * @param a_topic       消费的topic
     * @param a_partition   分区
     * @param a_seedBrokers hosts
     * @param a_port    链接端口
     * @throws Exception
     */
    public List<String> run(long a_maxReads, String a_topic, int a_partition, List<String> a_seedBrokers, int a_port)
            throws Exception {
        // Get point topic partition's meta
        PartitionMetadata metadata = findLeader(a_seedBrokers, a_port, a_topic, a_partition);
        if (metadata == null) {
            log.info("[SimpleKafkaConsumer.run()] - Can't find metadata for Topic and Partition. Exiting");
            return null;
        }
        if (metadata.leader() == null) {
            log.info("[SimpleKafkaConsumer.run()] - Can't find Leader for Topic and Partition. Exiting");
            return null;
        }
        String leadBroker = metadata.leader().host();
        String clientName = "Client_" + a_topic + "_" + a_partition;

        SimpleConsumer consumer = new SimpleConsumer(leadBroker, a_port, 100000, 64 * 1024, clientName);
        //long readOffset = getLastOffset(consumer, a_topic, a_partition, kafka.api.OffsetRequest.EarliestTime(),clientName);
        long readOffset=getOffsetOfTopicAndPartition(consumer,clientName,clientName,a_topic,a_partition);
        int numErrors = 0;

        List<String> datas=new ArrayList<>();
        while (a_maxReads > 0) {
            if (consumer == null) {
                consumer = new SimpleConsumer(leadBroker, a_port, 100000, 64 * 1024, clientName);
            }
            FetchRequest req = new FetchRequestBuilder().clientId(clientName)
                    .addFetch(a_topic, a_partition, readOffset, 100000).build();
            FetchResponse fetchResponse = consumer.fetch(req);

            if (fetchResponse.hasError()) {
                numErrors++;
                // Something went wrong!
                short code = fetchResponse.errorCode(a_topic, a_partition);
                log.info("[SimpleKafkaConsumer.run()] - Error fetching data from the Broker:" + leadBroker
                        + " Reason: " + code);
                if (numErrors > 5)
                    break;
                if (code == ErrorMapping.OffsetOutOfRangeCode()) {
                    // We asked for an invalid offset. For simple case ask for
                    // the last element to reset
                    readOffset = getLastOffset(consumer, a_topic, a_partition, kafka.api.OffsetRequest.LatestTime(),
                            clientName);
                    continue;
                }
                consumer.close();
                consumer = null;
                leadBroker = findNewLeader(leadBroker, a_topic, a_partition, a_port);
                continue;
            }
            numErrors = 0;
            //开始获取数据
            long numRead = 0;
            for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(a_topic, a_partition)) {
                long currentOffset = messageAndOffset.offset();
                if (currentOffset < readOffset) {
                    log.info("[SimpleKafkaConsumer.run()] - Found an old offset: " + currentOffset + " Expecting: "
                            + readOffset);
                    continue;
                }

                readOffset = messageAndOffset.nextOffset();
                ByteBuffer payload = messageAndOffset.message().payload();

                byte[] bytes = new byte[payload.limit()];
                payload.get(bytes);
                datas.add(new String(bytes, "UTF-8"));
                //提交offset
                boolean commintoffset = commintoffset(consumer,clientName, a_topic, a_partition, readOffset, clientName);
                log.info("消费数据offset"+String.valueOf(messageAndOffset.offset())+" and data is :"+new String(bytes, "UTF-8"));
                numRead++;
                a_maxReads--;

            }

            if (numRead == 0) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                }
            }
        }
        if (consumer != null)
            consumer.close();
        return datas;
    }

    /**
     * 获取上一次的offset信息
     * @param consumer  消费者
     * @param topic topic
     * @param partition f分区
     * @param whichTime 等待时间
     * @param clientName    访问的客户端的名称，group id
     * @return 返回当前的分区的消费的位置。
     */
    private static long getLastOffset(SimpleConsumer consumer, String topic, int partition, long whichTime, String clientName) {

        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);

        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));

        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo,
                kafka.api.OffsetRequest.CurrentVersion(), clientName);
        //获取到的相关offset信息
        OffsetResponse response = consumer.getOffsetsBefore(request);

        if (response.hasError()) {
            log.info("[SimpleKafkaConsumer.getLastOffset()] - Error fetching data Offset Data the Broker. Reason: "
                    + response.errorCode(topic, partition));
            return 0;
        }
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
    }


    /**
     * 获取最对应topic对应分区的上一次的offset
     * @param consumer
     * @param groupId   id
     * @param clientName    程序运行的名称
     * @param topic topic
     * @param partitionID   对应的分区
     * @return  获取到的offset long
     */
    private long getOffsetOfTopicAndPartition(SimpleConsumer consumer, String groupId, String clientName, String
            topic, int partitionID) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partitionID);
        List<TopicAndPartition> requestInfo = new ArrayList<TopicAndPartition>();
        requestInfo.add(topicAndPartition);
        OffsetFetchRequest request = new OffsetFetchRequest(groupId, requestInfo, 0, clientName);
        OffsetFetchResponse response = consumer.fetchOffsets(request);

        // 获取返回值
        Map<TopicAndPartition, OffsetMetadataAndError> returnOffsetMetadata = response.offsets();
        // 处理返回值
        if (returnOffsetMetadata != null && !returnOffsetMetadata.isEmpty()) {
            // 获取当前分区对应的偏移量信息
            OffsetMetadataAndError offset = returnOffsetMetadata.get(topicAndPartition);
            if (offset.error().code() == ErrorMapping.NoError()) {
                // 没有异常，表示是正常的，获取偏移量
                return offset.offset();
            } else {
                // 当Consumer第一次连接的时候(zk中不在当前topic对应数据的时候)，会产生UnknownTopicOrPartitionCode异常
                System.out.println("Error fetching data Offset Data the Topic and Partition. Reason: " + offset.error
                        ());
            }
        }

        // 所有异常情况直接返回0
        return 0;
    }


    /**
     * @param a_oldLeader
     * @param a_topic
     * @param a_partition
     * @param a_port
     * @return String
     * @throws Exception
     *             find next leader broker
     */
    private String findNewLeader(String a_oldLeader, String a_topic, int a_partition, int a_port) throws Exception {
        for (int i = 0; i < 3; i++) {
            boolean goToSleep = false;
            PartitionMetadata metadata = findLeader(m_replicaBrokers, a_port, a_topic, a_partition);
            if (metadata == null) {
                goToSleep = true;
            } else if (metadata.leader() == null) {
                goToSleep = true;
            } else if (a_oldLeader.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
                // first time through if the leader hasn't changed give
                // ZooKeeper a second to recover
                // second time, assume the broker did recover before failover,
                // or it was a non-Broker issue
                //
                goToSleep = true;
            } else {
                return metadata.leader().host();
            }
            if (goToSleep) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                }
            }
        }
        throw new Exception("Unable to find new leader after Broker failure. Exiting");
    }

    private PartitionMetadata findLeader(List<String> a_seedBrokers, int a_port, String a_topic, int a_partition) {
        PartitionMetadata returnMetaData = null;
        loop:
        for (String seed : a_seedBrokers) {
            SimpleConsumer consumer = null;
            try {
                consumer = new SimpleConsumer(seed, a_port, 100000, 64 * 1024, "leaderLookup");
                List<String> topics = Collections.singletonList(a_topic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                //获取当当前设备上的meta信息
                kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

                List<TopicMetadata> metaData = resp.topicsMetadata();
                for (TopicMetadata item : metaData) {
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        if (part.partitionId() == a_partition) {
                            returnMetaData = part;
                            break loop;
                        }
                    }
                }
            } catch (Exception e) {
                log.error("Error communicating with Broker [" + seed + "] to find Leader for [" + a_topic + ", "
                        + a_partition + "] Reason: " + e);
            } finally {
                if (consumer != null)
                    consumer.close();
            }
        }
        if (returnMetaData != null) {
            //获取到元信息后，将其备份的所有设备的IP添加到列表中去。
            m_replicaBrokers.clear();
            for (kafka.cluster.BrokerEndPoint replica : returnMetaData.replicas()) {
                m_replicaBrokers.add(replica.host());
            }
        }
        return returnMetaData;
    }


    /**
     * 提交offset的方法
     * @param consumer  消费者
     * @param groupid   id
     * @param topic     消费的主题
     * @param partition 消费的分区
     * @param offset    要提交的offset
     * @param clientName    程序运行时候的名称
     * @return  是否提交成功
     */
    public static boolean commintoffset(SimpleConsumer consumer,String groupid, String topic, int partition,long offset, String clientName){
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, OffsetAndMetadata> offsetCommitRequestinfo = new HashMap<TopicAndPartition, OffsetAndMetadata>();

        offsetCommitRequestinfo.put(topicAndPartition,new OffsetAndMetadata(new OffsetMetadata(offset, OffsetMetadata.NoMetadata()), offset, -1L));
        OffsetCommitRequest commitRequest = new OffsetCommitRequest(groupid, offsetCommitRequestinfo, 0, clientName, kafka.api.OffsetRequest.CurrentVersion());
        try {
            OffsetCommitResponse offsetCommitResponse = consumer.commitOffsets(commitRequest);
            if (offsetCommitResponse.hasError()){
                System.out.println(offsetCommitResponse.errorCode(topicAndPartition));
            }
            return true;
        }catch (Exception e){
            return false;
        }

    }

}