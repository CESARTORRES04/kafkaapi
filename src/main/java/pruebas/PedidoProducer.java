package pruebas;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import java.util.Properties;
import com.kafkaprocon.kafkaapi.model.*;
import com.kafkaprocon.kafkaapi.config.*;

public class PedidoProducer {

    private static final String TOPIC = "pedidos-topic";
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {

        // Antes tenías: Properties props = new Properties(); props.put(...)
        // Ahora:
        Properties props = KafkaSecurityConfig.base();

        // Configuración específica del producer (se agrega encima de la seguridad)
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                  "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                  "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.ACKS_CONFIG,             "all");
        props.put(ProducerConfig.RETRIES_CONFIG,          3);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,       16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG,        5);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        String[] productos = {"Laptop", "Mouse", "Teclado", "Monitor", "Audífonos"};

        for (int i = 1001; i <= 1020; i++) {
            PedidoEvent pedido = new PedidoEvent(
                i,
                "Cliente-" + i,
                productos[i % productos.length],
                1,
                (i % 3 == 0) ? 25000.0 : 5000.0,
                "2026-05-30 10:00:00"
            );

            String json   = mapper.writeValueAsString(pedido);
            String key    = "pedido-" + i;

            ProducerRecord<String, String> record =
                new ProducerRecord<>(TOPIC, key, json);

            producer.send(record, (metadata, ex) -> {
                if (ex != null) {
                    System.err.println("ERROR al enviar: " + ex.getMessage());
                } else {
                    System.out.printf("Enviado → topic=%s partición=%d offset=%d%n",
                        metadata.topic(), metadata.partition(), metadata.offset());
                }
            });

            Thread.sleep(200);
        }

        producer.flush();
        producer.close();
        System.out.println("Todos los pedidos enviados");
    }
}