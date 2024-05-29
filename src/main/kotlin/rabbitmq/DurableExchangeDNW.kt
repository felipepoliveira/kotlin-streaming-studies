package com.inohoster.rabbitmq

import com.rabbitmq.client.Channel
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DeliverCallback
import com.rabbitmq.client.MessageProperties
import kotlinx.coroutines.*
import java.nio.charset.StandardCharsets

private const val DURABLE_EXCHANGE_NAME : String = "durable_exchange"
private const val DURABLE_EXCHANGE_QUEUE_NAME : String = "durable_exchange-queue"
private const val TIME_TO_WAIT_UNTIL_CONSUMERS_CREATION = 5000.toLong()
private const val MESSAGE_TTL_MS : Long = 60000

fun startDurableExchange() {
    val factory = ConnectionFactory()
    factory.host = "localhost"
    factory.username = "root"
    factory.password = "root132"

    runBlocking {
        val publisherJob = startPublisher(factory)
        val consumersJobs = startConsumers(factory, 3)

        publisherJob.join()
        consumersJobs.awaitAll()
    }
}

private fun assertQueueExists(channel: Channel, queueName: String,
                              durable: Boolean,
                              exclusiveToConnection: Boolean,
                              deleteWhenNotUsed: Boolean,
                              arguments: Map<String, String> = mapOf(),
                              alwaysDelete: Boolean = false) {
    if (alwaysDelete) {
        channel.queueDelete(queueName)
    }

    channel.queueDeclare(queueName, durable, exclusiveToConnection, deleteWhenNotUsed, arguments)
}

private fun startConsumers(factory: ConnectionFactory, numberOfConsumers: Int) = runBlocking {
    // Validated minimum amount of consumers
    if (numberOfConsumers < 0)
        throw Exception("Negative number of consumers not allowed: $numberOfConsumers")

    if (TIME_TO_WAIT_UNTIL_CONSUMERS_CREATION > 0) {
        println("Waiting $TIME_TO_WAIT_UNTIL_CONSUMERS_CREATION milliseconds until consumers creation")
        delay(TIME_TO_WAIT_UNTIL_CONSUMERS_CREATION)
    }

    coroutineScope {
        val jobs = List(numberOfConsumers) { index ->
            async {
                println("Started consumer $index")

                // configure channel
                val channel = factory.newConnection().createChannel()
                channel.exchangeDeclare(DURABLE_EXCHANGE_NAME, "fanout", true)

                val arguments = mapOf("x-message-ttl" to MESSAGE_TTL_MS)

                val queueName = "DURABLE_EXCHANGE_QUEUE_NAME-${index}"
                assertQueueExists(
                    channel, queueName,
                    durable = true, exclusiveToConnection = false, deleteWhenNotUsed = true, alwaysDelete = true
                )
                channel.queueBind(queueName, DURABLE_EXCHANGE_NAME, "")

                val deliverCallback = DeliverCallback { _, delivery ->
                    val message = String(delivery.body, StandardCharsets.UTF_8)
                    println(" [x] Consumer $index Received: '$message'")
                }

                channel.basicConsume(queueName, true, deliverCallback) { _ -> }
            }
        }
        return@coroutineScope jobs
    }
}

private fun startPublisher(factory: ConnectionFactory) : Job {
    return GlobalScope.launch {
        coroutineScope {
            factory.newConnection().use { connection ->
                val channel = connection.createChannel()
                channel.exchangeDeclare(DURABLE_EXCHANGE_NAME, "fanout", true)

                var messagesSent = 0
                while (true) {
                    val message = "msg: ${++messagesSent}"
                    channel.basicPublish(DURABLE_EXCHANGE_NAME, "", MessageProperties.PERSISTENT_TEXT_PLAIN, message.toByteArray());
                    delay(1000)
                }
            }
        }
    }
}