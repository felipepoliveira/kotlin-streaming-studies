package com.inohoster.kafka

import com.fasterxml.jackson.databind.ObjectMapper
import com.inohoster.kafka.msg.KafkaMessageWrapper
import com.inohoster.kafka.msg.accounts.UserAccountUpdated
import kotlinx.coroutines.*
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.*

private const val TOPIC: String = "accounts-userAccount-updated"

fun startKafkaSingleProducerMultipleConsumers() {
    val kafkaProps = Properties().apply {
        put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer::class.java.name)
        put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer::class.java.name)
        put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    }

    runBlocking {
        val producerJob = startProducer(this, kafkaProps)
        val consumerJob = startConsumer(kafkaProps, numberOfConsumers = 4)

        producerJob.join()
        consumerJob.awaitAll()
    }
}

private fun startProducer(scope: CoroutineScope, kafkaProps: Properties): Job {
    return scope.launch {
        coroutineScope {
            KafkaProducer<String, ByteArray>(kafkaProps).use { producer ->
                while (true) {

                    // the message transport
                    val messageWrapper = KafkaMessageWrapper.asJson(UserAccountUpdated(
                        id = 1,
                        email = "teste@email.com"
                    ))

                    // Send the record to the topic
                    val record = ProducerRecord<String, ByteArray>(TOPIC, messageWrapper.serializeAsJsonByteArray())
                    producer.send(record) { metadata, exception ->

                        // if an error occur, trace it
                        if (exception != null) {
                            exception.printStackTrace()
                            return@send
                        }

                        println("Sent message: $metadata")
                    }

                    delay(5000)
                }
            }
        }
    }
}

private fun startConsumer(kafkaProps: Properties, numberOfConsumers: Int = 1) = runBlocking {
    if (numberOfConsumers < 0) {
        throw Exception("The number of consumer can not be negative. $numberOfConsumers given")
    }

    val jobs = List(numberOfConsumers) { index ->
        async(Dispatchers.Default) {
            val consumerId = "consumer-$index"
            println("Initializing $consumerId")

            val consumerKafkaProps = Properties().apply {
                putAll(kafkaProps)
                put(ConsumerConfig.GROUP_ID_CONFIG, consumerId)
            }

            KafkaConsumer<String, ByteArray>(consumerKafkaProps).use { consumer ->
                consumer.subscribe(listOf(TOPIC))
                while (true) {
                    val records = consumer.poll(Duration.of(1, ChronoUnit.SECONDS))
                    for (record in records) {
                        val message = record.value()
                        println("$consumerId received: ${String(message)}")
                    }
                }
            }
        }
    }

    return@runBlocking jobs
}


