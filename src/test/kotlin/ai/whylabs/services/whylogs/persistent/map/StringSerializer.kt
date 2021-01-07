package ai.whylabs.services.whylogs.persistent.map

import ai.whylabs.services.whylogs.persistent.Serializer
import java.nio.charset.Charset

internal class StringSerializer : Serializer<String> {
    override fun deserialize(bytes: ByteArray): String {
        return bytes.toString(Charset.forName("utf-8"))
    }

    override fun serialize(t: String): ByteArray {
        return t.toByteArray(Charset.forName("utf-8"))
    }
}

