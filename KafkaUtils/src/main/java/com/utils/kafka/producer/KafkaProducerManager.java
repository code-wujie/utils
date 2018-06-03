package com.utils.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;


/**
* kafka producer
*/
public class KafkaProducerManager implements  Closeable, Serializable {

	private static final Logger LOGGER = LogManager.getLogger(KafkaProducerManager.class);
	private static final long serialVersionUID = -2957939705672109579L;

	/**kafka broker**/
	private static String broker;
	/**生产者**/
	private Producer<String, String> producer;

	/**实例**/
	private final static Hashtable<String, KafkaProducerManager> producers = new Hashtable<String, KafkaProducerManager>();


	private KafkaProducerManager(Properties properties){
		producer = new KafkaProducer<String, String>(properties);
	}

	/**
	 * 单例
	 * @return	单例
	 * @throws Exception
	 */
	public static KafkaProducerManager getInstance(Properties properties){
		broker = properties.getProperty("bootstrap.servers");
		if(!producers.containsKey(broker)){
			synchronized (Producer.class) {
				if(!producers.containsKey(broker)){
					producers.put(broker, new KafkaProducerManager(properties));
				}
			}
		}
		return producers.get(broker);
	}

	/**
	 * 发送消息
	 * @param topic		主题
	 * @param message	消息
	 */
	public void push(String topic, String message) {

		LOGGER.debug("send message to topic[{}], message:{}", topic, message);
		try {
			producer.send(new ProducerRecord<>(topic, message));
		} catch (Exception e) {
			LOGGER.error("send message to topic[" + topic + "] error, continue",e);
		}
	}

	/**
	 * 发送消息
	 *
	 * @param topic   主题
	 * @param message 消息
	 * @param var3    Callback
	 */
	public void push(String topic, String message,Callback var3) {

		LOGGER.debug("send message to topic[{}], message:{}", topic, message);
		try {
			producer.send(new ProducerRecord<>(topic, message),var3);
		} catch (Exception e) {
			LOGGER.error("send message to topic[" + topic + "] error, continue");
		}
	}

	/**
	 * 发送消息
	 *
	 * @param topic    主题
	 * @param messages 消息
	 */
	public void push(String topic, List<String> messages){

		try {
			LOGGER.debug("send message to topic[" + topic + "]");
			for(String msg : messages){
				producer.send(new ProducerRecord<>(topic, msg));
			}
		} catch (Exception e) {
			LOGGER.error("Kafka Producer Exception：", e);
		}
	}

	/**
	 * 发送消息
	 *
	 * @param topic    主题
	 * @param messages 消息
	 * @param var3     Callback
	 */
	public void push(String topic, List<String> messages,Callback var3){

		try {
			LOGGER.debug("send message to topic[" + topic + "]");
			for(String msg : messages){
				producer.send(new ProducerRecord<>(topic, msg),var3);
			}
		} catch (Exception e) {
			LOGGER.error("Kafka Producer Exception：", e);
		}
	}


	/**
	 * 发送消息
	 *
	 * @param producerRecords 消息
	 */
	public void send(List<ProducerRecord<String,String>> producerRecords) {

		try {
			for (ProducerRecord producerRecord : producerRecords) {
				LOGGER.debug("send message to topic[" + producerRecord.topic() + "]");
				producer.send(producerRecord);
			}
		} catch (Exception e) {
			LOGGER.error("Kafka Producer Exception：", e);
		}
	}

	/**
	 * 发送消息
	 *
	 * @param producerRecord 消息
	 */
	public void send(ProducerRecord<String,String> producerRecord) {

		try {
			LOGGER.debug("send message to topic[" + producerRecord.topic() + "]");
			producer.send(producerRecord);
		} catch (Exception e) {
			LOGGER.error("Kafka Producer Exception：", e);
		}
	}
	
	/**
	 * 关闭生产者
	 */
	@Override
	public void close() {
		if(null != producer){
			producer.close();
		}
	}


	/**
	 * 立即发送缓存数据
	 */
	public void flush() {
		if (null != producer) {
			producer.flush();
		}
	}

}
