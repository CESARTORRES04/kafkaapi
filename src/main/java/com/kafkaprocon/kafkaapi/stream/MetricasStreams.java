package com.kafkaprocon.kafkaapi.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import java.time.Duration;
import java.util.Properties;
import com.kafkaprocon.kafkaapi.config.*;
import com.kafkaprocon.kafkaapi.model.*;

public class MetricasStreams {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) {

        // ── Seguridad + configuración de Streams ──────────────────
        Properties props = KafkaSecurityConfig.base(); // <── misma línea que en los demás

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "metricas-app");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                  Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                  Serdes.String().getClass());

        // ── Topología ─────────────────────────────────────────────
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> pagos = builder.stream("pagos-topic");

        // 1. Ventas totales por producto
        pagos
            .filter((k, v) -> {
                try {
                    PagoEvent p = mapper.readValue(v, PagoEvent.class);
                    return "APROBADO".equals(p.getEstado());
                } catch (Exception e) {
                    return false;
                }
            })
            .map((k, v) -> {
                try {
                    PagoEvent p = mapper.readValue(v, PagoEvent.class);
                    return KeyValue.pair(p.getProducto(),
                                        String.valueOf(p.getMonto()));
                } catch (Exception e) {
                    return KeyValue.pair("error", "0");
                }
            })
            .groupByKey()
            .reduce(
                (v1, v2) -> String.valueOf(
                    Double.parseDouble(v1) + Double.parseDouble(v2)),
                Materialized.as("ventas-por-producto")
            )
            .toStream()
            .to("metricas-ventas-topic");

        // 2. Pedidos por minuto (ventana temporal)
        pagos
            .groupByKey()
            .windowedBy(
                TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1))
            )
            .count(Materialized.as("pedidos-por-minuto"))
            .toStream()
            .map((wk, count) -> KeyValue.pair(
                wk.key() + "@" + wk.window().startTime(),
                count.toString()
            ))
            .to("metricas-ventas-topic");

        // ── Arrancar y manejar cierre limpio ──────────────────────
        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Cerrando Kafka Streams...");
            streams.close();
        }));

        streams.start();
        System.out.println("✓ MetricasStreams corriendo con SASL_SSL...");
    }
}