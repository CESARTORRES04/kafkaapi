package com.kafkaprocon.kafkaapi.config;

import java.util.Properties;

public class KafkaSecurityConfig {

    // ── Ajusta estos valores a tu entorno ──────────────────────────
    private static final String BOOTSTRAP_SERVERS   = "localhost:9092";
    private static final String USERNAME            = "admin";
    private static final String PASSWORD            = "admin-secret";
    private static final String TRUSTSTORE_LOCATION = "D:/kafka-security/kafka.server.truststore.jks";
    private static final String TRUSTSTORE_PASSWORD = "secret123";
    private static final String TRUSTSTORE_TYPE     = "PKCS12";
    // ──────────────────────────────────────────────────────────────

    /**
     * Propiedades base de seguridad (compartidas por producer, consumer y streams).
     */
    public static Properties base() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);

        // Seguridad
        props.put("security.protocol",  "SASL_SSL");
        props.put("sasl.mechanism",     "PLAIN");
        props.put("sasl.jaas.config",
            "org.apache.kafka.common.security.plain.PlainLoginModule required " +
            "username=\"" + USERNAME + "\" " +
            "password=\"" + PASSWORD + "\";");

        // SSL — truststore
        props.put("ssl.truststore.location", TRUSTSTORE_LOCATION);
        props.put("ssl.truststore.password", TRUSTSTORE_PASSWORD);
        props.put("ssl.truststore.type",     TRUSTSTORE_TYPE);

        // Deshabilita verificación de hostname (igual que tu admin.properties)
        props.put("ssl.endpoint.identification.algorithm", "");

        return props;
    }
}