package ub.exo.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class StockPriceConsumer {
    public static void main(String[] args) {
        // Configuration du consommateur Kafka
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "stock-price-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Pour lire depuis le début du topic

        // Créer un consommateur Kafka
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // S'abonner à un topic
        consumer.subscribe(Collections.singletonList("stock-prices"));

        // Boucle pour consommer les messages
        try {
            Map<String, Double> packageValues = new HashMap<>(); // Stocke la valeur des packages

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    updatePackageValues(packageValues, record);
                }

                // Affiche les valeurs calculées des packages
                packageValues.forEach((key, value) ->
                        System.out.printf("%s, Valeur Totale: %.2f\n", key, value));
            }
        } finally {
            consumer.close();
        }
    }

    private static void updatePackageValues(Map<String, Double> packageValues, ConsumerRecord<String, String> record) {
        String stockSymbol = record.key();
        double price = extractPrice(record.value());

        // Mettre à jour la valeur totale du package pour chaque symbole d'action
        packageValues.merge(stockSymbol, price, Double::sum);
    }

    private static double extractPrice(String stockData) {
        String[] parts = stockData.split(",");
        return Double.parseDouble(parts[2]); // On sait que le prix est le 3ème élément retourné par le producteur
    }
}
