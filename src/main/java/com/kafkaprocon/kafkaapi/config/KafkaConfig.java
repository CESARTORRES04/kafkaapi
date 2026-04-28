package com.kafkaprocon.kafkaapi.config;

import java.util.Properties;

public class KafkaConfig {
	public static Properties getProducerConfig() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		return props;
		
	}
	
	public static Properties getConsumerConfig(String groupId, String instanceId) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		
		props.put("group.id", groupId);
		//Grupo Estatico
		props.put("group.instance.id", instanceId);
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("auto.offset.reset", "earliest");
		props.put("enable.auto.commit", "false");
		//Rebalanceo
		props.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
		
		return props;
		
	}
	
	
}
