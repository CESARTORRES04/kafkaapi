package pruebas;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import com.kafkaprocon.kafkaapi.config.KafkaSecurityConfig;
import com.kafkaprocon.kafkaapi.model.*;

public class ConsumerPago {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {

        // ── Consumer ──────────────────────────────────────────────
        Properties cProps = KafkaSecurityConfig.base();
        cProps.put(ConsumerConfig.GROUP_ID_CONFIG, "pagos-group4");
        cProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                   "org.apache.kafka.common.serialization.StringDeserializer");
        cProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                   "org.apache.kafka.common.serialization.StringDeserializer");
        cProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // ── Producer interno (para publicar pagos) ────────────────
        Properties pProps = KafkaSecurityConfig.base();
        pProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                   "org.apache.kafka.common.serialization.StringSerializer");
        pProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                   "org.apache.kafka.common.serialization.StringSerializer");
        pProps.put(ProducerConfig.ACKS_CONFIG, "all");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(cProps);
        KafkaProducer<String, String> producer = new KafkaProducer<>(pProps);

        consumer.subscribe(Collections.singletonList("pedidos-topic"));

        System.out.println("ConsumerPago escuchando pedidos-topic...");

        while (true) {
            ConsumerRecords<String, String> records =
                consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records) {
                try {
                    PedidoEvent pedido = mapper.readValue(record.value(), PedidoEvent.class);

                    // Simular validación de pago
                    boolean aprobado = pedido.getPrecio() < 30000;

                    PagoEvent pago = new PagoEvent(
                        pedido.getPedidoId(),
                        aprobado ? "APROBADO" : "RECHAZADO",
                        pedido.getPrecio(),
                        pedido.getProducto()
                    );

                    String pagoJson = mapper.writeValueAsString(pago);
                    producer.send(new ProducerRecord<>("pagos-topic",
                        "pago-" + pedido.getPedidoId(), pagoJson));

                    System.out.printf("Pago procesado: pedido=%d estado=%s%n",
                        pedido.getPedidoId(), pago.getEstado());

                } catch (Exception e) {
                	System.err.println("Error procesando registro: " + e.getMessage());
                    e.printStackTrace();
                    
                    if (e.getMessage() != null) {
                        producer.send(new ProducerRecord<>("errores-topic",
                            "error", e.getMessage()));
                    }
                }
            }
        }
    }
}