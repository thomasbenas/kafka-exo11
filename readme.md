#  MASTER BDIA BDED EXO 11 KAFKA 

## Technos 

- Java 8
- Kafka 2.1.1

## Init

### Installation 

- Avoir kafka installé sur sa machine (https://kafka.apache.org/downloads).
- Mettre en place ZooKeeper (pour la gestion du cluster)
```shell
bin/zookeeper-server-start.sh config/zookeeper.properties
```
Dans un autre terminal, démarrer Kafka : 
```shell
bin/kafka-server-start.sh config/server.properties
```
### Intéraction entre producteurs et consommateurs 
Pour que les producteurs puissent envoyer aux consommateurs des messages,
il faut au moins un topic, que l'on crée comme suit :
````shell
bin/kafka-topics.sh --create --topic stock-prices --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
````