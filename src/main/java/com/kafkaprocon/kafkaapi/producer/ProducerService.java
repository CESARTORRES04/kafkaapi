package com.kafkaprocon.kafkaapi.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.*;

public class ProducerService {
	private final KafkaProducer<String,String> producer;
	
	public ProducerService(Properties props) {
		this.producer = new KafkaProducer<>(props);		
	}
	
	public void enviar(String topic, String key, String mensaje) {
		ProducerRecord<String, String> record = new ProducerRecord<>(topic, key , mensaje);
		
		producer.send(record,(metadata,exception)-> {
			if(exception == null) {
				System.out.println("Enviado a la particion:" + metadata.partition());
			}else {
				System.out.println("Error Enviando Mensaje");
				exception.printStackTrace();
			}
		});
		
		
	}
	
	public void cerrar() {
		producer.close();
	}
	
	
}
