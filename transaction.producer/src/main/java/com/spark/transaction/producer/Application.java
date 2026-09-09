package com.spark.transaction.producer;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.Locale;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

@SpringBootApplication
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Bean
    CommandLineRunner run(TransactionProducer producer) {
        return args -> {
            int numThreads = 8; // Ajuste conforme os núcleos da sua CPU
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            AtomicLong counter = new AtomicLong(1);

            System.out.println("🚀 Produtor de Alta Performance Iniciado! Alvo: 100k msg/s...");

            for (int t = 0; t < numThreads; t++) {
                executor.submit(() -> {
                    Random rand = new Random();
                    double baseLat = -23.550520;
                    double baseLon = -46.633308;

                    while (!Thread.currentThread().isInterrupted()) {
                        long count = counter.getAndIncrement();
                        String transactionId = "TX_" + count;
                        String userId = "USER_" + rand.nextInt(1000);
                        double amount = 10.0 + (5000.0 - 10.0) * rand.nextDouble();
                        long timestamp = System.currentTimeMillis() / 1000;

                        boolean isFraud = rand.nextInt(100) < 5; 
                        double lat = isFraud ? -3.119027 : baseLat + (rand.nextDouble() * 0.01);
                        double lon = isFraud ? -60.021731 : baseLon + (rand.nextDouble() * 0.01);

                        String jsonPayload = String.format(Locale.US,
                            "{\"transaction_id\":\"%s\",\"user_id\":\"%s\",\"amount\":%.2f,\"timestamp\":%d,\"lat\":%.6f,\"lon\":%.6f,\"prev_lat\":%.6f,\"prev_lon\":%.6f}",
                            transactionId, userId, amount, timestamp, lat, lon, baseLat, baseLon
                        );

                        // Envia sem System.out.println (Log no console destrói a performance)
                        producer.sendTransaction(userId, jsonPayload);
                    }
                });
            }
        };
    }
}