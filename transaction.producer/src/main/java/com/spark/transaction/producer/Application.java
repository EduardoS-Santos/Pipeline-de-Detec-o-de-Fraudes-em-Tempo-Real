package com.spark.transaction.producer;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.Locale;
import java.util.Random;

@SpringBootApplication
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Bean
    CommandLineRunner run(TransactionProducer producer) {
        return args -> {
            Random rand = new Random();
            int count = 1;

            System.out.println("🚀 Produtor Iniciado! Gerando transações no Schema Bruto...");

            while (true) {
                String transactionId = "TX_" + (1000 + count);
                String userId = "USER_" + rand.nextInt(50);
                double amount = 10.0 + (5000.0 - 10.0) * rand.nextDouble();
                long timestamp = System.currentTimeMillis() / 1000;

                // Coordenadas base (São Paulo)
                double baseLat = -23.550520;
                double baseLon = -46.633308;

                // 20% de chance de fraude (> 200 KM, ex: Manaus)
                boolean isFraud = rand.nextInt(100) < 20; 
                double lat = isFraud ? -3.119027 : baseLat + (rand.nextDouble() * 0.01 - 0.005);
                double lon = isFraud ? -60.021731 : baseLon + (rand.nextDouble() * 0.01 - 0.005);

                // JSON VÁLIDO obedecendo o Schema TransacaoBruta
                String jsonPayload = String.format(Locale.US,
                    "{\"transaction_id\":\"%s\",\"user_id\":\"%s\",\"amount\":%.2f,\"timestamp\":%d,\"lat\":%.6f,\"lon\":%.6f,\"prev_lat\":%.6f,\"prev_lon\":%.6f}",
                    transactionId, userId, amount, timestamp, lat, lon, baseLat, baseLon
                );

                producer.sendTransaction(userId, jsonPayload);

                count++;
                Thread.sleep(2000); // 1 transação a cada 2s
            }
        };
    }
}