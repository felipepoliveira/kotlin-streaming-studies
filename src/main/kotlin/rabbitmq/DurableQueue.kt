package com.inohoster.rabbitmq

import com.rabbitmq.client.Channel
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DeliverCallback
import kotlinx.coroutines.*
import java.nio.charset.StandardCharsets

private const val QUEUE_NAME : String = "durable_queue"
private const val MESSAGE_TTL_MS : Long = 60000
private const val TIME_TO_WAIT_UNTIL_CONSUMERS_CREATION = 15000.toLong()

fun startDurableQueue() {
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

private fun assertQueueExists(channel: Channel, alwaysDelete: Boolean = false) {
    if (alwaysDelete) {
        channel.queueDelete(QUEUE_NAME)
    }
    val arguments = mapOf("x-message-ttl" to MESSAGE_TTL_MS)
    channel.queueDeclare(QUEUE_NAME, true, false, false, arguments)
}

private fun startConsumers(factory: ConnectionFactory, numberOfConsumers: Int) = runBlocking {
    // Validated minimum amount of consumers
    if (numberOfConsumers < 1)
        throw Exception("At least 1 consumer should be created when using this function")


    if (TIME_TO_WAIT_UNTIL_CONSUMERS_CREATION > 0) {
        println("Waiting $TIME_TO_WAIT_UNTIL_CONSUMERS_CREATION milliseconds until consumers creation")
        delay(TIME_TO_WAIT_UNTIL_CONSUMERS_CREATION)
    }

    coroutineScope {
        val jobs = List(numberOfConsumers) { index ->
            async {
                println("Started consumer $index")

                val channel = factory.newConnection().createChannel()
                assertQueueExists(channel)
                val deliverCallback = DeliverCallback { _, delivery ->
                    val message = String(delivery.body, StandardCharsets.UTF_8)
                    println(" [x] Consumer $index Received: '$message'")
                }
                channel.basicConsume(QUEUE_NAME, true, deliverCallback) { _ -> }
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
                assertQueueExists(channel, alwaysDelete = true)

                var messagesSent = 0
                while (true) {
                    val message = "msg: ${++messagesSent}"
                    channel.basicPublish("", QUEUE_NAME, null, message.toByteArray());
                    delay(1000)
                }
            }
        }
    }
}