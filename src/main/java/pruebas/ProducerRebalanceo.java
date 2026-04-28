package pruebas;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerRebalanceo {
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("acks", "all");
		props.put("retries", "3");

		KafkaProducer<String, String> producer = new KafkaProducer<>(props);

		String[] mensajes = { "1", "2", "3", "error", "5", "6" };

		for (int i = 0; i < mensajes.length; i++) {

			ProducerRecord<String, String> record = new ProducerRecord<>("mensajes-java-particiones",
					"cliente" + (i % 2), mensajes[i]);

			producer.send(record, (metadata, exception) -> {
				if (exception == null) {

					System.out.println("Enviado a la particion:" + metadata.partition());
				} else {
					exception.printStackTrace();
				}

			}

			);

		}
		producer.close();

	}
}
