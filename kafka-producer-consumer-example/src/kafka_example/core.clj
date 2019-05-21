(ns kafka-example.core
  (:gen-class)
  (:require [clojure.tools.logging :as log]
            [environ.core :refer [env]])
  (:import [org.apache.kafka.clients.admin AdminClient AdminClientConfig NewTopic]
           org.apache.kafka.clients.consumer.KafkaConsumer
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
           [org.apache.kafka.common.serialization StringDeserializer StringSerializer]))

(defn create-topics!
  "Create the topic "
  [bootstrap-server topics partitions replication]
  (let [config {AdminClientConfig/BOOTSTRAP_SERVERS_CONFIG bootstrap-server}
        adminClient (AdminClient/create config)
        new-topics (map #(NewTopic. % partitions replication) topics)]
    (.createTopics adminClient new-topics)))

(defn- build-consumer
  "Create the consumer instance to consume
from the provided kafka topic name"
  [consumer-topic bootstrap-server]
  (let [consumer-props
        {"bootstrap.servers", bootstrap-server
         "group.id",          "test-group"
         "key.deserializer",  StringDeserializer
         "value.deserializer", StringDeserializer
         "auto.offset.reset", "earliest"
         "enable.auto.commit", "true"
         "security.protocol", "SASL_PLAINTEXT"
         "sasl.mechanism", "SCRAM-SHA-256"
         "sasl.jaas.config","org.apache.kafka.common.security.scram.ScramLoginModule required username=USER_READ password=PASSWORD;"}]

    (doto (KafkaConsumer. consumer-props)
      (.subscribe [consumer-topic]))))

(defn- build-producer
  "Create the kafka producer to send on messages received"
  [bootstrap-server]
  (let [producer-props {"value.serializer", StringSerializer
                        "key.serializer" , StringSerializer
                        "bootstrap.servers", bootstrap-server
                        "security.protocol", "SASL_PLAINTEXT"
                        "sasl.mechanism", "SCRAM-SHA-256"
                        "sasl.jaas.config","org.apache.kafka.common.security.scram.ScramLoginModule required username=USER_WRITE password=PASSWORD;"}]
    (KafkaProducer. producer-props)))

(defn -main
  [& args]

  (.addShutdownHook (Runtime/getRuntime) (Thread. #(log/info "Shutting down")))

  (def consumer-topic "example-consumer-topic")
  (def producer-topic "example-produced-topic")
  (def test-message "my-test-message")
  (def bootstrap-server (env :bootstrap-server "localhost:9092"))
  (def zookeeper-hosts (env :zookeeper-hosts "localhost:2181"))

  ;; Create the example topics.
  ;; Reminder: In prod env applications can't guarantee the existence of the topics, because doesn't have creation topic permission.
  (log/infof "Creating the topics %s" [producer-topic consumer-topic])
  (create-topics! bootstrap-server [producer-topic consumer-topic] 1 1)

  (def consumer (build-consumer consumer-topic bootstrap-server))

  (def producer (build-producer bootstrap-server))

  (log/infof "Starting the kafka example app. With topic consuming topic %s and producing to %s"
             consumer-topic producer-topic)

  (while true
    ;; Produces some message to consumer topic.
    (log/info "Producing message to consumer topic: ")
    (.send producer (ProducerRecord. consumer-topic test-message))
    (let [records (.poll consumer 100)]
      ;; Subscribe to consumer-topic, that already received 1 message
      (doseq [record records]
        (log/info "Received message" (str "Value: " (.value record)))
        ;; Produce the same message, consumed, in another topic
        (.send producer (ProducerRecord. producer-topic (.value record)))))

    (.commitAsync consumer)))
