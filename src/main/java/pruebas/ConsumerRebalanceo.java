package pruebas;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerRebalanceo {
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		
		props.put("group.id", "grupo-java");
		//Grupo Estatico
		props.put("group.instance.id", "consumer-1");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("auto.offset.reset", "earliest");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms","5000");
		//Rebalanceo
		props.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
		
		KafkaConsumer<String,String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Collections.singletonList("mensajes-java-particiones"));
		System.out.println("Consumer iniciado");
		
		try {
			while(true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
				
				for(ConsumerRecord<String, String> record : records) {
					
					
						
						System.out.println(
								"Consumer:" + props.getProperty("group.instance.id") +
								"Key:" + record.key() +
								"| Valor:" + record.value() +
								"| Partition:" + record.partition() +
								"| OffSet:" + record.offset());
						
						
						

						
					try {
						System.out.println("Procensando Mensaje");
						
						if("error".equalsIgnoreCase(record.value())) {
							throw new RuntimeException("Error procesando mensaje");
						}
						
					}catch (Exception e) {
						
						System.out.println("Error Procensando Mensaje" + record.value());
					}
				}
				
				
				
			}
			
		}catch (Exception e) {
			System.out.println("Error en el consumer");
			e.printStackTrace();
		}finally {
			consumer.close();
		}
	}
}
