package me.imatveev.kafkademo;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Slf4j
@EnableKafka
@SpringBootApplication
public class KafkaDemoApplication {
    private static final String TOPIC_NAME = "quickstart";

    public static void main(String[] args) {
        SpringApplication.run(KafkaDemoApplication.class, args);
    }

    @Autowired
    private KafkaTemplate<String, String> template;


    @Bean
    public CommandLineRunner runner() {
        return args -> {
            sendMessage("Hello!)");
            sendMessage("from");
            sendMessage("Spring Boot");
        };
    }

    @KafkaListener(topics = TOPIC_NAME)
    public void listenGroupFoo(String message) {
        log.info("Received Message: {}", message);
    }

    private void sendMessage(String message) {

        final ListenableFuture<SendResult<String, String>> future = template.send(TOPIC_NAME, message);

        future.addCallback(new ListenableFutureCallback<>() {

            @Override
            public void onSuccess(SendResult<String, String> result) {
                log.info("Sent message=[{}] with offset=[{}]", message, result.getRecordMetadata().offset());
            }

            @Override
            public void onFailure(@NonNull Throwable ex) {
                log.error("Unable to send message=[{}] due to : {}", message, ex.getMessage());
            }
        });
    }
}
