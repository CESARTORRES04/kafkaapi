package com.kafkaprocon.kafkaapi;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import com.kafkaprocon.kafkaapi.producer.*;
import com.kafkaprocon.kafkaapi.config.KafkaConfig;
import com.kafkaprocon.kafkaapi.consumer.*;

@SpringBootApplication
public class KafkaapiApplication {

	public static void main(String[] args) {
		String topic = "mensajes-java-particiones";
		
		//Producer
		ProducerService producer = new ProducerService(KafkaConfig.getProducerConfig());
		producer.enviar(topic, "usuario1", "Compra procesada");
		producer.enviar(topic, "admin2", "Pedido entregado");
		producer.enviar(topic, "invitado3", "Dato guardado");
		
		producer.cerrar();
		
		
		//Consumer
		ConsumerService consumer = new ConsumerService
				(KafkaConfig.getConsumerConfig("grupo-java", "consumer-1"));
		
		consumer.consumir(topic);
		
		
		
		
	}

}
