import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import java.util.*;

/**
 * Created by WJ on 2017/11/13.
 */
public class Kafka_Consumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        // zookeeper 配置
        props.put("zookeeper.connect", "192.168.200.21:12181,192.168.200.22:12181,192.168.200.23:12181");

        // group 代表一个消费组
        props.put("group.id", "jd-group");

        // zk连接超时
        props.put("zookeeper.session.timeout.ms", "4000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "smallest");
        // 序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");

        ConsumerConfig config = new ConsumerConfig(props);

        ConsumerConnector javaConsumerConnector = Consumer.createJavaConsumerConnector(config);

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put("task3", new Integer(1));

        StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
        StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());

        Map<String, List<KafkaStream<String, String>>> consumerMap = javaConsumerConnector.createMessageStreams(topicCountMap,
                keyDecoder, valueDecoder);
        KafkaStream<String, String> stream = consumerMap.get("task3").get(0);
        ConsumerIterator<String, String> it = stream.iterator();
        while (it.hasNext())
            System.out.println(it.next().message());
    }
}
