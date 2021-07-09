package br.com.vvaug.simplekafkaclient.service;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class MessageSender {

	private static final String ECOMMERCE_NEW_ORDER_TOPIC = "ECOMMERCE_NEW_ORDER";
	
	private static final String ECOMMERCE_SEND_EMAIL_TOPIC = "ECOMMERCE_SEND_EMAIL";

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		
		var producer = new KafkaProducer<String, String>(properties());
		
		var value = "123123, 666000, 994512";
		
		sendMessage(producer, value, value, ECOMMERCE_NEW_ORDER_TOPIC); //same key n value
		
		var mail = "Thank you for your order! We are processing your order.";
		
		sendMessage(producer, mail, mail, ECOMMERCE_SEND_EMAIL_TOPIC);
	}

	private static <K,V> void sendMessage(KafkaProducer<K, V> producer, K key, V value, String topic) throws InterruptedException, ExecutionException {
		
		var record = new ProducerRecord<>(topic, key, value);
		
		producer.send(record, (data, err) -> {
			if (Objects.nonNull(err)) {
				err.printStackTrace();
				return;
			}
			System.out.println("SUCCESS SENDING :: " + data.topic() + " partition: " + data.partition() + " | offset: " + data.offset() + " | timestamp: " + data.timestamp());
		}).get();
	
	}
	
	private static Properties properties() {
		var properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		//String to bytes
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return properties;
	} 
}
