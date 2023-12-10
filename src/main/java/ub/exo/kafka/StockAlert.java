package ub.exo.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StockAlert {

    public static void main(String[] args) {
        // Configuration avec Kafka Streams */
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stock-alerts");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> stockStream = builder.stream("stock-prices");

        KStream<String, String> alertsStream = stockStream.filter((key, value) -> {
            double price = extractPrice(value);
            return price < 1500; // Condition d'alerte, ici si le prix passe sous les 1500
        });

        alertsStream.foreach((key, value) -> System.out.println("Alerte sur le " + key + ", valeur totale " + value));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static double extractPrice(String stockData) {
        String[] parts = stockData.split(",");
        return Double.parseDouble(parts[2]); // On sait que le prix est le 3ème élément retourné par le producteur
    }
}

