package ai.whylabs.services.whylogs.core

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.whylogs.core.DatasetProfile
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.time.Instant

class ProfileEntryTests {

    @Test
    fun `profile entries serialize`() {
        val arbitraryMilliTime1 = 1610135186681
        val arbitraryMilliTime2 = arbitraryMilliTime1 + 1
        val serializer = ProfileEntrySerializer()
        val sessionTimestamp = Instant.ofEpochMilli(arbitraryMilliTime1)
        val dataTimestamp = Instant.ofEpochMilli(arbitraryMilliTime2)

        val entry = ProfileEntry(
            orgId = "orgid",
            datasetId = "dataset id",
            profile = DatasetProfile("session id", sessionTimestamp, dataTimestamp, mapOf(), mapOf()),
        )

        val serialized = serializer.serialize(entry)

        val deserializedEntry = serializer.deserialize(serialized)

        val mapper = jacksonObjectMapper()
        // TODO there is no equals on the DatasetProfile class apparently so serialize them to strings
        // and compare the values, unfortunately
        Assertions.assertEquals(
            mapper.writeValueAsString(entry.profile),
            mapper.writeValueAsString(deserializedEntry.profile)
        )

        Assertions.assertEquals(deserializedEntry.datasetId, entry.datasetId)
        Assertions.assertEquals(deserializedEntry.orgId, entry.orgId)
    }
}