package com.inohoster.kafka.msg

import com.fasterxml.jackson.databind.ObjectMapper

class KafkaMessageWrapper(
    /**
     * The data of the kafka message
     */
    val data: String,

    /**
     * MIME type used to transport the data
     */
    val dataMimeType: String,

    /**
     *The model type of the data (if the data is structured)
     */
    val dataModelType: String?,
) {
    companion object {

        fun asJson(data: Any, objectMapper: ObjectMapper = defaultObjectMapper()) = KafkaMessageWrapper(
            data = objectMapper.writeValueAsString(data),
            dataMimeType = "application/json",
            dataModelType = data.javaClass.simpleName
        )

        private fun defaultObjectMapper(): ObjectMapper {
            val objectMapper = ObjectMapper()
            return objectMapper
        }
    }

    /**
     * Serialize this instance as JSON Byte Array
     */
    fun serializeAsJsonByteArray(objectMapper: ObjectMapper = defaultObjectMapper()) = objectMapper.writeValueAsBytes(this)
}