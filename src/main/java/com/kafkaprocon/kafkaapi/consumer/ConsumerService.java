package com.kafkaprocon.kafkaapi.consumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.*;

public class ConsumerService {
	private final KafkaConsumer<String,String> consumer;
	
	public ConsumerService(Properties props) {
		this.consumer = new KafkaConsumer<>(props);
	}
	
	public void consumir(String topic) {
		consumer.subscribe(Collections.singletonList(topic));
		
		while(true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
			
			for(ConsumerRecord<String, String> record : records) {
				
				try {
					
					System.out.println(
							"Key:" + record.key() +
							"| Valor:" + record.value() +
							"| Partition:" + record.partition() +
							"| OffSet:" + record.offset());
					
					if("error".equalsIgnoreCase(record.value())) {
						throw new RuntimeException("Error procesando mensaje");
					}
					
					consumer.commitSync();
					
				}catch (Exception e) {
					
					System.out.println("Error Procensando Mensaje");
				}
			}
			
			
			
		}
	}
	
}
