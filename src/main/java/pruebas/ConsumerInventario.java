package pruebas;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import com.kafkaprocon.kafkaapi.config.*;
import com.kafkaprocon.kafkaapi.model.*;

public class ConsumerInventario {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {

        // ── Consumer ──────────────────────────────────────────────
        Properties cProps = KafkaSecurityConfig.base();
        cProps.put(ConsumerConfig.GROUP_ID_CONFIG, "inventario-group");
        cProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                   "org.apache.kafka.common.serialization.StringDeserializer");
        cProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                   "org.apache.kafka.common.serialization.StringDeserializer");
        cProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // ── Producer (publica actualizaciones de stock) ───────────
        Properties pProps = KafkaSecurityConfig.base();
        pProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                   "org.apache.kafka.common.serialization.StringSerializer");
        pProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                   "org.apache.kafka.common.serialization.StringSerializer");
        pProps.put(ProducerConfig.ACKS_CONFIG, "all");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(cProps);
        KafkaProducer<String, String> producer = new KafkaProducer<>(pProps);

        consumer.subscribe(Collections.singletonList("pedidos-topic"));

        System.out.println("ConsumerInventario escuchando pedidos-topic...");

        // Stock simulado en memoria
        java.util.Map<String, Integer> stock = new java.util.HashMap<>();
        stock.put("Laptop",    10);
        stock.put("Mouse",     50);
        stock.put("Teclado",   30);
        stock.put("Monitor",   15);
        stock.put("Audífonos", 25);

        while (true) {
            ConsumerRecords<String, String> records =
                consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records) {
                try {
                    PedidoEvent pedido = mapper.readValue(record.value(), PedidoEvent.class);

                    String producto  = pedido.getProducto();
                    int    cantidad  = pedido.getCantidad();
                    int    stockActual = stock.getOrDefault(producto, 0);

                    // Reducir stock
                    String estadoStock;
                    if (stockActual >= cantidad) {
                        stock.put(producto, stockActual - cantidad);
                        estadoStock = "ACTUALIZADO";
                        System.out.printf("✓ Stock actualizado → producto=%s | antes=%d | después=%d%n",
                            producto, stockActual, stock.get(producto));
                    } else {
                        estadoStock = "SIN_STOCK";
                        System.out.printf("✗ Sin stock → producto=%s | disponible=%d | solicitado=%d%n",
                            producto, stockActual, cantidad);
                    }

                    // Publicar resultado en inventario-topic
                    InventarioEvent inventario = new InventarioEvent(
                        pedido.getPedidoId(),
                        producto,
                        stock.getOrDefault(producto, 0),
                        estadoStock
                    );

                    String json = mapper.writeValueAsString(inventario);
                    producer.send(
                        new ProducerRecord<>("inventario-topic",
                            "inv-" + pedido.getPedidoId(), json),
                        (metadata, ex) -> {
                            if (ex != null) {
                                System.err.println("Error publicando inventario: " + ex.getMessage());
                            }
                        }
                    );

                } catch (Exception e) {
                    System.err.println("Error procesando pedido: " + e.getMessage());
                    producer.send(new ProducerRecord<>("errores-topic",
                        "error-inventario", e.getMessage()));
                }
            }
        }
    }
}