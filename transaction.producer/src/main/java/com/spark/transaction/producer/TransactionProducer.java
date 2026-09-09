package com.spark.transaction.producer;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class TransactionProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public TransactionProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendTransaction(String userId, String jsonTransaction) {
        // Envia para o tópico 'transacoes.brutas' usando userId como Key
        kafkaTemplate.send("transacoes.brutas", userId, jsonTransaction);
    }
}