package com.example.pulsardemo

import org.apache.pulsar.client.api.MessageId
import org.apache.pulsar.client.api.PulsarClient
import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.client.api.SubscriptionType
import org.apache.pulsar.common.api.proto.PulsarApi
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class PulsarDemoApplication : CommandLineRunner {
    override fun run(vararg args: String?) {
        val client = PulsarClient.builder()
                .serviceUrl("plusar://localhost:6650")
                .build()

        val producer = client.newProducer(Schema.STRING)
                .topic("test")
                .create()

        producer.send("hello")
        producer.send("how")
        producer.send("are")
        producer.send("you?")

        val reader = client.newReader(Schema.STRING)
                .topic("test")
                .startMessageId(MessageId.earliest)
                .create()

        while (true) {
            val message = reader.readNext()
            println(message.value)
        }
    }
}

fun main(args: Array<String>) {
    runApplication<PulsarDemoApplication>(*args)
}
