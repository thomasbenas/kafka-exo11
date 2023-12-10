package ub.exo.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class StockPackageProducer {
    private static final Map<String, String> stockMarketMap = new HashMap<>();
    static {
        stockMarketMap.put("Package1", "AAPL");
        stockMarketMap.put("Package2", "GOOGL");
        stockMarketMap.put("Package3", "MSFT");
        stockMarketMap.put("Package4", "AMZN");
        stockMarketMap.put("Package5", "BABA");
    }

    public static void main(String[] args) {
        /* Configuration du producteur Kafka */
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        /* Simulation de l'envoi des données de packages d'actions */
        try {
            while (true) {
                for (String packageId : stockMarketMap.keySet()) {
                    String packageData = generatePackageData(packageId);
                    producer.send(new ProducerRecord<>("stock-prices", packageId, packageData));
                }
                Thread.sleep(1000); // Pause entre chaque envoi
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    private static String generatePackageData(String packageId) {
        Random random = new Random();
        int numberOfShares = 10 + random.nextInt(91); // Génère un nombre aléatoire d'actions
        double pricePerShare = 100 + (200 - 100) * random.nextDouble(); // Prix aléatoire par action
        return stockMarketMap.get(packageId) + "," + numberOfShares + "," + pricePerShare;
    }
}
