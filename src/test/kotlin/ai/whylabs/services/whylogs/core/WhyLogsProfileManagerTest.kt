package ai.whylabs.services.whylogs.core

import ai.whylabs.services.whylogs.core.config.EnvVars
import ai.whylabs.services.whylogs.core.writer.WriteResult
import ai.whylabs.services.whylogs.core.writer.Writer
import ai.whylabs.services.whylogs.persistent.queue.PopSize
import com.whylogs.core.DatasetProfile
import io.mockk.every
import io.mockk.mockkObject
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Instant

private const val sessionId = "123"
private const val orgId = "org-1"

class WhyLogsProfileManagerTest {

    lateinit var manager: WhyLogsProfileManager
    lateinit var profileStore: ProfileStore

    @BeforeEach
    fun init() = runBlocking {
        mockkObject(EnvVars)
        every { EnvVars.instance } returns TestEnvVars()
        profileStore = ProfileStore(
            sessionId = sessionId,
        )
        manager = WhyLogsProfileManager(
            currentTime = Instant.ofEpochMilli(1),
            writer = FakeWriter(),
            profileStore = profileStore,
            writeOnStop = false,
        )
    }

    @AfterEach
    fun after() = runBlocking {
        // Clear out any cached entries since this uses a real sqlite backend.
        profileStore.profiles.map.reset {
            emptyMap()
        }
    }

    @Test
    fun `buffered items merge into the map correctly`() = runBlocking {
        val bufferedMap = profileStore.profiles
        val (a) = createTestData()
        val (_, request) = a

        // Add some stuff
        bufferedMap.merge(request)
        bufferedMap.merge(request)

        // Merge bufferedMap from the queue into the map
        CompletableDeferred<Unit>().apply {
            bufferedMap.mergeBuffered(PopSize.All, this)
            await()
        }

        // Make sure buffered items are in the map
        val expectedKey = ProfileKey(
            orgId = orgId,
            datasetId = "model-1",
            normalizedTags = listOf(Pair("tag1", "value1"), Pair("tag2", "value2")),
            sessionTime = Instant.ofEpochMilli(1),
            windowStartTime = Instant.ofEpochMilli(2),
        )

        // Define what the profile should look like
        val profile = DatasetProfile(
            sessionId,
            Instant.ofEpochMilli(1),
            Instant.ofEpochMilli(2),
            mapOf("tag1" to "value1", "tag2" to "value2", OrgIdTag to orgId, DatasetIdTag to "model-1"),
            mapOf()
        ).apply {
            // Update the profile like we expect it to be updated
            merge(request.request, setOf())
            merge(request.request, setOf())
        }

        val expectedValue = ProfileEntry(profile = profile, orgId = orgId, datasetId = "model-1")
        val expected = mapOf(expectedKey to expectedValue)

        // Get the map content to make assertion against
        bufferedMap.map.reset { mapContent ->
            Assertions.assertEquals(mapContent.size, expected.size, "Different map sizes")

            // DatasetProfile has no equals() method so I have to do this the hard way.
            val actualValue = mapContent.values.toList()[0]
            Assertions.assertEquals(actualValue.datasetId, expectedValue.datasetId, "Different dataset ids")
            Assertions.assertEquals(actualValue.orgId, expectedValue.orgId, "Different org ids")
            actualValue.profile.assertEquals(expectedValue.profile)
            mapContent
        }

        // Make sure its not in the queue anymore
        profileStore.config.queue.pop(PopSize.All) {
            Assertions.assertEquals(0, it.size, "Should have nothing left to pop")
        }
    }

    @Test
    fun `profiles are stored separately if they have different tags and modeles`() = runBlocking {
        val bufferedMap = profileStore.profiles

        val (a, b, c, d) = createTestData()
        val (expectedKeyA, requestA, profileA) = a
        val (expectedKeyB, requestB, profileB) = b
        val (expectedKeyC, requestC, profileC) = c
        val (expectedKeyD, requestD, profileD) = d

        bufferedMap.merge(requestA)
        bufferedMap.merge(requestA)

        bufferedMap.merge(requestB)
        bufferedMap.merge(requestB)

        bufferedMap.merge(requestC)
        bufferedMap.merge(requestC)

        bufferedMap.merge(requestD)
        bufferedMap.merge(requestD)

        // Merge from the queue into the map
        CompletableDeferred<Unit>().apply {
            bufferedMap.mergeBuffered(PopSize.All, this)
            await()
        }

        // Update the profiles like we expect them to be updated
        profileA.merge(requestA.request, setOf())
        profileA.merge(requestA.request, setOf())
        profileA.merge(requestA.request, setOf())

        profileB.merge(requestB.request, setOf())
        profileB.merge(requestB.request, setOf())

        profileC.merge(requestC.request, setOf())
        profileC.merge(requestC.request, setOf())

        profileD.merge(requestD.request, setOf())
        profileD.merge(requestD.request, setOf())

        // Create expected values
        val expectedValueA = ProfileEntry(profile = profileA, orgId = orgId, datasetId = "model-1")
        val expectedValueB = ProfileEntry(profile = profileB, orgId = orgId, datasetId = "model-1")
        val expectedValueC = ProfileEntry(profile = profileC, orgId = orgId, datasetId = "model-1")
        val expectedValueD = ProfileEntry(profile = profileD, orgId = orgId, datasetId = "model-2")
        val expected = mapOf(
            expectedKeyA to expectedValueA,
            expectedKeyB to expectedValueB,
            expectedKeyC to expectedValueC,
            expectedKeyD to expectedValueD,
        )

        // Get the map content to make assertion against
        bufferedMap.map.reset { mapContent ->
            Assertions.assertEquals(mapContent.size, expected.size, "Different map sizes")
            Assertions.assertEquals(mapContent.size, 4, "Expected 4 unique profile entries")

            // DatasetProfile has no equals() method so I have to do this the hard way.
            val actualValues = mapContent.entries.toList().sortedBy { it.key.toString() }
            val expectedValues = expected.entries.toList().sortedBy { it.key.toString() }

            for (i in actualValues.indices) {
                val actualValue = actualValues[i].value
                val expectedValue = expectedValues[i].value

                Assertions.assertEquals(actualValue.datasetId, expectedValue.datasetId, "Different dataset ids")
                Assertions.assertEquals(actualValue.orgId, expectedValue.orgId, "Different org ids")
                actualValue.profile.assertEquals(expectedValue.profile)
            }

            mapContent
        }

        // Make sure its not in the queue anymore
        profileStore.config.queue.pop(PopSize.All) {
            Assertions.assertEquals(0, it.size, "Should have nothing left to pop")
        }
    }
}

class FakeWriter : Writer {
    override suspend fun write(profile: DatasetProfile, orgId: String, datasetId: String): WriteResult {
        return WriteResult(type = "FakeResult")
    }
}

private fun DatasetProfile.assertEquals(other: DatasetProfile) {
    // TODO actually implement a real equals on DatasetProfile. This check is OK for now.
    val thisColumns = this.columns.entries.toList().sortedBy { it.key }
    val otherColumns = this.columns.entries.toList().sortedBy { it.key }

    Assertions.assertEquals(thisColumns.size, otherColumns.size, "Profiles have different amount of columns.")

    for (i in (thisColumns.indices)) {
        Assertions.assertEquals(thisColumns[i].value.counters.count, otherColumns[i].value.counters.count, "counters")
        Assertions.assertEquals(thisColumns[i].value.columnName, otherColumns[i].value.columnName, "columnNames")
    }

    Assertions.assertEquals(this.tags, other.tags, "tags")
    Assertions.assertEquals(this.dataTimestamp, other.dataTimestamp, "dataTimestamp")
    Assertions.assertEquals(this.metadata, other.metadata, "metadata")
    Assertions.assertEquals(this.sessionId, other.sessionId, "sessionId")
}

data class TestItem(val key: ProfileKey, val requestBuffered: BufferedLogRequest, val profile: DatasetProfile)

fun createTestData(): List<TestItem> {
    // Make a bunch of requests that should end up getting stored separately because of their tags
    val tagsA = mapOf("tag1" to "value1", "tag2" to "value2")
    val requestA = BufferedLogRequest(
        request = LogRequest(
            datasetId = "model-1",
            tags = tagsA,
            single = tagsA,
            multiple = null
        ),
        sessionTime = Instant.ofEpochMilli(1),
        windowStartTime = Instant.ofEpochMilli(2),
    )
    val keyA = ProfileKey(
        orgId = orgId,
        datasetId = "model-1",
        normalizedTags = tagsA.entries.toList().map { Pair(it.key, it.value) },
        sessionTime = Instant.ofEpochMilli(1),
        windowStartTime = Instant.ofEpochMilli(2),
    )
    val profileA = DatasetProfile(
        sessionId,
        Instant.ofEpochMilli(1),
        Instant.ofEpochMilli(2),
        tagsA + mapOf(OrgIdTag to orgId, DatasetIdTag to "model-1"),
        mapOf()
    )

    val tagsB = mapOf("tag1" to "a", "tag2" to "b")
    val requestB = BufferedLogRequest(
        request = LogRequest(
            datasetId = "model-1",
            tags = tagsB,
            single = tagsB,
            multiple = null
        ),
        sessionTime = Instant.ofEpochMilli(1),
        windowStartTime = Instant.ofEpochMilli(2),
    )
    val keyB = ProfileKey(
        orgId = orgId,
        datasetId = "model-1",
        normalizedTags = tagsB.entries.toList().map { Pair(it.key, it.value) },
        sessionTime = Instant.ofEpochMilli(1),
        windowStartTime = Instant.ofEpochMilli(2),
    )
    val profileB = DatasetProfile(
        sessionId,
        Instant.ofEpochMilli(1),
        Instant.ofEpochMilli(2),
        tagsB + mapOf(OrgIdTag to orgId, DatasetIdTag to "model-1"),
        mapOf()
    )

    val tagsC = mapOf("tag1" to "value1")
    val requestC = BufferedLogRequest(
        request = LogRequest(
            datasetId = "model-1",
            tags = tagsC,
            single = tagsC,
            multiple = null
        ),
        sessionTime = Instant.ofEpochMilli(1),
        windowStartTime = Instant.ofEpochMilli(2),
    )
    val keyC = ProfileKey(
        orgId = orgId,
        datasetId = "model-1",
        normalizedTags = tagsC.entries.toList().map { Pair(it.key, it.value) },
        sessionTime = Instant.ofEpochMilli(1),
        windowStartTime = Instant.ofEpochMilli(2),
    )
    val profileC = DatasetProfile(
        sessionId,
        Instant.ofEpochMilli(1),
        Instant.ofEpochMilli(2),
        tagsC + mapOf(OrgIdTag to orgId, DatasetIdTag to "model-1"),
        mapOf()
    )

    val requestD = BufferedLogRequest(
        request = LogRequest(
            datasetId = "model-2", // Different model
            tags = tagsC, // Same tags
            single = tagsC,
            multiple = null
        ),
        sessionTime = Instant.ofEpochMilli(1),
        windowStartTime = Instant.ofEpochMilli(2),
    )
    val keyD = ProfileKey(
        orgId = orgId,
        datasetId = "model-2",
        normalizedTags = tagsC.entries.toList().map { Pair(it.key, it.value) },
        sessionTime = Instant.ofEpochMilli(1),
        windowStartTime = Instant.ofEpochMilli(2),
    )
    val profileD = DatasetProfile(
        sessionId,
        Instant.ofEpochMilli(1),
        Instant.ofEpochMilli(2),
        tagsC + mapOf(OrgIdTag to orgId, DatasetIdTag to "model-2"),
        mapOf()
    )

    return listOf(
        TestItem(keyA, requestA, profileA),
        TestItem(keyB, requestB, profileB),
        TestItem(keyC, requestC, profileC),
        TestItem(keyD, requestD, profileD)
    )
}
