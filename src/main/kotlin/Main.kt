package com.inohoster

import com.inohoster.kafka.startKafkaSingleProducerMultipleConsumers
import com.inohoster.rabbitmq.startDurableExchange
import com.inohoster.rabbitmq.startDurableQueue
import com.inohoster.rabbitmq.startExchange
import com.inohoster.rabbitmq.startQueue

fun main() {
    startKafkaSingleProducerMultipleConsumers()
}