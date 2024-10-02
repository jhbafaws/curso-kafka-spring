package com.finytec.curso.kafka.spring.config;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableScheduling
public class KafkaConfiguration {

    @Bean
    public Map<String, Object> producerProperties() {

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        props.put(ProducerConfig.RETRIES_CONFIG,0);
//        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
//        props.put(ProducerConfig.LINGER_MS_CONFIG,1);
//        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }

    public KafkaTemplate<String, String> kafkaTemplate() {

        DefaultKafkaProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(producerProperties());
        producerFactory.addListener(new MicrometerProducerListener<String, String>(meterRegistry())); // AGREGA meterRegistry
        KafkaTemplate<String, String> template = new KafkaTemplate<>(producerFactory);

        return template;

    }

    @Bean
    public Map<String, Object> consumerProperties() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "finytec-group");
//        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
//        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100);
//        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
//        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        return props;
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerProperties());
    }

//    @Bean
//    public ConcurrentKafkaListenerContainerFactory<String, String> listenerContainerFactory() {
//        ConcurrentKafkaListenerContainerFactory<String, String> listenerContainerFactory = new ConcurrentKafkaListenerContainerFactory<>();
//        listenerContainerFactory.setConsumerFactory(consumerFactory());
//        return listenerContainerFactory;
//    }

    @Bean(name = "listenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String> listenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> listenerContainerFactory = new ConcurrentKafkaListenerContainerFactory<>();
        listenerContainerFactory.setConsumerFactory(consumerFactory());
        listenerContainerFactory.setBatchListener(true);
        listenerContainerFactory.setConcurrency(5); // genera 5 hilos
        return listenerContainerFactory;
    }

    @Bean
    public MeterRegistry meterRegistry() {
        PrometheusMeterRegistry prometheusMeterRegistry = new
                PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        return prometheusMeterRegistry;
    }

    // INCLUIR METRICA EN PRODUCER
}


