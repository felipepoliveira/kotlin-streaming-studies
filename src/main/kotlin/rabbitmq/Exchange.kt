package com.inohoster.rabbitmq

import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DeliverCallback
import kotlinx.coroutines.*
import java.nio.charset.StandardCharsets

private const val EXCHANGE_NAME : String = "exchange"
private const val TIME_TO_WAIT_UNTIL_CONSUMERS_CREATION = 15000.toLong()

fun startExchange() {
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
                channel.exchangeDeclare(EXCHANGE_NAME, "fanout")
                val queueName = channel.queueDeclare().queue
                channel.queueBind(queueName, EXCHANGE_NAME, "")

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
                channel.exchangeDeclare(EXCHANGE_NAME, "fanout")

                var messagesSent = 0
                while (true) {
                    val message = "msg: ${++messagesSent}"
                    channel.basicPublish(EXCHANGE_NAME, "", null, message.toByteArray());
                    delay(1000)
                }
            }
        }
    }
}