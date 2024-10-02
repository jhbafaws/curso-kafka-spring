package com.finytec.curso.kafka.spring;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.List;

@SpringBootApplication
@EnableKafka
//public class CursoKafkaSpringApplication implements CommandLineRunner {
public class CursoKafkaSpringApplication {

    private static final Logger log = LoggerFactory.getLogger(CursoKafkaSpringApplication.class);
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @Autowired
    private MeterRegistry meterRegistry;

    public static void main(String[] args) {
        SpringApplication.run(CursoKafkaSpringApplication.class, args);
    }

//    @KafkaListener(topics = "finytec-topic", groupId = "finytec-group")
//    public void listen(String message) {
//        log.info("Message received {} ", message);
//    }

    @KafkaListener(id = "finytecId", autoStartup = "true", topics = "finytec-topic", containerFactory = "listenerContainerFactory", groupId = "finytec-group",
            properties = {"max.poll.interval.ms:4000", "max.poll.records:50"}
    )

//    public void listen(List<String> messages) {
//        log.info("Start reading messages");
//        for(String message: messages){
//            log.info("Message batchListener - setBatchListener received {} ", messages);
//        }
//        log.info("End reading messages");
//    }

    public void listen(List<ConsumerRecord<String, String>> messages) {
 //       log.info("Start reading messages");
        for (ConsumerRecord<String, String> message : messages) {
            log.info("Partition batchListener - setBatchListener received = {}, Offset = {}, key ={}, value ={} ",
                    message.partition(),
                    message.offset(),
                    message.key(),
                    message.value());
        }
 //       log.info("End Batch messages");
    }

    @Scheduled(fixedDelay = 2000, initialDelay = 100)
    public void sendKafkaMessages() {
     //   log.info("Finytec Rocks");
        for (int i = 0; i < 100; i++) {
            kafkaTemplate.send("finytec-topic", String.valueOf(i), "sample message " + i);
        }
    }


    @Scheduled(fixedDelay = 2000, initialDelay = 500)
    public void printMetrics(){

      List<Meter> metric =  meterRegistry.getMeters();

//      for(Meter meter: metric){
//          log.info("meter = {} ", meter.getId().getName());
//      }

        try {
            // Busca la métrica como contador en lugar de function counter
            double count = meterRegistry.get("kafka.producer.record.send.total").functionCounter().count();
            log.info("Count: {}", count);
        } catch (Exception e) {
            log.error("Failed to fetch Kafka metrics: ", e);
        }

    }

//    @Override
//    public void run(String... args) throws Exception {

        // IMPLEMENTACION NORMAL
        //	kafkaTemplate.send("finytec-topic", "Sample NORMAL message");

        // IMPLEMENTACION CON CALLBACKS ASINCRONO

//        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send("finytec-topic", "Este es mi Mensaje JHBA 2024 septiembre ");
//
//        future.thenAccept(result -> {
//            System.out.println("Message sent successfully: " + result.getProducerRecord());
//        }).exceptionally(ex -> {
//            System.err.println("Message failed to send: " + ex.getMessage());
//            return null;
//        });

        // PRODUCER SINCRONO

        //   kafkaTemplate.send(new ProducerRecord<String, String>("finytec-topic", "Producer Sincorno GET")).get();

        // kafkaTemplate.send(new ProducerRecord<String, String>("finytec-topic", "Producer SINCRONO GET 10, TimeUnit.SECONDS ")).get(10, TimeUnit.SECONDS);

        //schedule1


        // ANTES DE METRIC
//        for (int i = 0; i < 100; i++) {
//
//            kafkaTemplate.send("finytec-topic", String.valueOf(i), "sample message " + i);
//        }
//
//        log.info("Waiting to start 5s");
//        Thread.sleep(5000);
//        log.info("Waiting 5s finished");
//        registry.getListenerContainer("finytecId").start();
//        Thread.sleep(5000);
//        log.info("Process STOPPING");
//        registry.getListenerContainer("finytecId").stop();

        //schediulesd and metric


  //  }



}
