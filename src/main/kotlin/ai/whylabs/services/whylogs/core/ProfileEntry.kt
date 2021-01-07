package ai.whylabs.services.whylogs.core

import ai.whylabs.services.whylogs.persistent.Serializer
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.whylogs.core.DatasetProfile
import com.whylogs.core.message.DatasetProfileMessage
import java.nio.charset.Charset
import java.util.Base64

/**
 * Container class that caches the dataset profiles and associated metadata for each request.
 */
data class ProfileEntry(val profile: DatasetProfile, val orgId: String, val datasetId: String)

// TODO use something besides jackson. It ends up converting everything to base64 despite
// us being able to store as binary. A little wasteful.
private class ProfileEntryJacksonSerializer : StdSerializer<ProfileEntry>(ProfileEntry::class.java) {
    private val charset = Charset.forName("utf-8")
    override fun serialize(value: ProfileEntry, gen: JsonGenerator, provider: SerializerProvider) {
        gen.writeStartObject()
        val serializedProfile = value.profile.toProtobuf().build().toByteArray()
        gen.writeBinaryField("profile", serializedProfile)

        gen.writeStringField("orgId", value.orgId)
        gen.writeStringField("datasetId", value.datasetId)
    }
}

private class ProfileEntryJacksonDeserializer : StdDeserializer<ProfileEntry>(ProfileEntry::class.java) {
    override fun deserialize(p: JsonParser, ctxt: DeserializationContext): ProfileEntry {
        val tree = ctxt.readTree(p)

        val profileBase64 = tree.get("profile").asText()
        val profileBytes = Base64.getDecoder().decode(profileBase64)
        val profile = DatasetProfile.fromProtobuf(DatasetProfileMessage.parseFrom(profileBytes))

        val orgId = tree.get("orgId").textValue()
        val datasetId = tree.get("datasetId").textValue()

        return ProfileEntry(profile = profile, orgId = orgId, datasetId = datasetId)
    }
}

private val mapper =
    jacksonObjectMapper().registerModule(
        SimpleModule().addDeserializer(
            ProfileEntry::class.java,
            ProfileEntryJacksonDeserializer()
        ).addSerializer(
            ProfileEntry::class.java,
            ProfileEntryJacksonSerializer()
        )
    )

internal class ProfileEntrySerializer : Serializer<ProfileEntry> {

    override fun serialize(t: ProfileEntry): ByteArray {
        return mapper.writeValueAsBytes(t)
    }

    override fun deserialize(bytes: ByteArray): ProfileEntry {
        return mapper.readValue(bytes, ProfileEntry::class.java)
    }
}

internal class TagSerializer : Serializer<TagsKey> {
    override fun serialize(t: TagsKey): ByteArray {
        return mapper.writeValueAsBytes(t)
    }

    override fun deserialize(bytes: ByteArray): TagsKey {
        return mapper.readValue(bytes, TagsKey::class.java)
    }
}
