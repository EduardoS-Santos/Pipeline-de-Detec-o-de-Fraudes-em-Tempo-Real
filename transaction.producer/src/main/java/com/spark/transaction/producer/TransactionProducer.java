package com.spark.transaction.producer;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Random;
import java.util.UUID;

@Service
public class TransactionProducer {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    // Dispara a cada 1 segundo (1000 ms)
    @Scheduled(fixedRate = 1000)
    public void generateTransaction() {
        String transactionJson = String.format(
            "{\"transaction_id\":\"%s\", \"user_id\":\"%s\", \"amount\":%.2f, \"timestamp\":%d}",
            UUID.randomUUID().toString(),
            "USER_" + new Random().nextInt(100),
            100 + (15000 - 100) * new Random().nextDouble(),
            System.currentTimeMillis()
        );

        kafkaTemplate.send("transacoes-brutas", transactionJson);
        System.out.println("Enviado: " + transactionJson);
    }
} 