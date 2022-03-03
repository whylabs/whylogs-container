package ai.whylabs.services.whylogs.kafka

import com.fasterxml.jackson.module.kotlin.convertValue
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.common.serialization.Deserializer

class JSONObjectDeserializer : Deserializer<Map<String, Any>> {
    private val mapper = jacksonObjectMapper()

    override fun deserialize(topic: String, data: ByteArray): Map<String, Any> {
        val tree = mapper.readTree(data)
        return mapper.convertValue(tree)
    }
}