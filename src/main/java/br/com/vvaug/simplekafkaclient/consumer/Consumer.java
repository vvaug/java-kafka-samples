package br.com.vvaug.simplekafkaclient.consumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class Consumer {
	
	public static void main(String[] args) throws InterruptedException {
		var consumer = new KafkaConsumer<String, String>(properties());
		consumer.subscribe(Collections.singletonList("ECOMMERCE_NEW_ORDER"));
	  while (true) {
		  var records = consumer.poll(Duration.ofMillis(100));
		  if (records.isEmpty()) {
				continue;
			}
		
		  records.forEach(record -> {
			  System.out.println("Message received :: " + record.value() + " | " + record.partition() + " | " + record.offset() + " | " + record.timestamp());
		  });
	  }
		
	}

	private static Properties properties() {
		var properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		//String to bytes
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, Consumer.class.getName());
		return properties;
	}

}
