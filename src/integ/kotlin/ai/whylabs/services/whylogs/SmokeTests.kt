package ai.whylabs.services.whylogs

import ai.whylabs.services.whylogs.core.IEnvVars
import ai.whylabs.services.whylogs.core.LogRequest
import ai.whylabs.services.whylogs.core.MultiLog
import ai.whylabs.services.whylogs.core.ProfileWritePeriod
import ai.whylabs.services.whylogs.core.WriteLayer
import ai.whylabs.services.whylogs.core.WriterTypes
import ai.whylabs.services.whylogs.persistent.queue.PopSize
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.time.Instant
import java.time.temporal.ChronoUnit

class SmokeTests {
    private val client = TestClient(TestEnvVars())

    @Test
    fun `writing profiles works`() = runBlocking {
        client.withServer {
            val datasetTimestamp = 1648751959098

            val data = LogRequest(
                datasetId = "foo",
                timestamp = datasetTimestamp,
                tags = mapOf(
                    "tag1" to "value1"
                ),
                single = null,
                multiple = MultiLog(
                    columns = listOf("Brand", "Price"),
                    data = listOf(
                        listOf("Honda Civic", 22000),
                        listOf("Toyota Corolla", 25000),
                        listOf("Ford Focus", 27000),
                        listOf("Audi A4", 35000)
                    )
                )
            )
            client.track(data)

            val profileResponse = client.writeProfiles()
            val profile = client.loadProfiles(profileResponse.profilePaths).first()

            val expectedTimestamp = Instant.ofEpochMilli(datasetTimestamp).truncatedTo(ChronoUnit.HOURS)
            Assertions.assertEquals(1, profileResponse.profilePaths.size, "number of profiles paths returned")
            Assertions.assertEquals(1, profileResponse.profilesWritten, "number of profiles written")
            Assertions.assertEquals(expectedTimestamp, profile.dataTimestamp, "dataset timestamp of the written profile")
            Assertions.assertEquals(setOf("Brand", "Price"), profile.columns.keys, "")
            Assertions.assertEquals(mapOf("whylogs.tag.tag1" to "value1", "datasetId" to "foo", "orgId" to "nothing"), profile.tags, "")
        }
    }

    @Test
    fun `writing multiple profiles works`() = runBlocking {
        client.withServer {
            val datasetTimestamp = 1648751959098

            val firstRequest = LogRequest(
                datasetId = "foo",
                timestamp = datasetTimestamp,
                tags = mapOf("tag1" to "value1"),
                single = null,
                multiple = MultiLog(
                    columns = listOf("Brand", "Price"),
                    data = listOf(
                        listOf("Honda Civic", 22000),
                        listOf("Toyota Corolla", 25000),
                        listOf("Ford Focus", 27000),
                        listOf("Audi A4", 35000)
                    )
                )
            )
            client.track(firstRequest)

            val secondDataPoint = LogRequest(
                datasetId = "foo",
                timestamp = datasetTimestamp,
                single = null,
                tags = mapOf("tag1" to "value1"),
                multiple = MultiLog(
                    columns = listOf("Model", "Price"),
                    data = listOf(
                        listOf("Ford Focus", 27000),
                        listOf("Audi A4", 35000)
                    )
                )
            )
            client.track(secondDataPoint)

            // Data point for an hour later
            val nextHourRequest = LogRequest(
                datasetId = "foo",
                timestamp = Instant.ofEpochMilli(datasetTimestamp).plus(1, ChronoUnit.HOURS).toEpochMilli(),
                single = null,
                tags = mapOf(),
                multiple = MultiLog(
                    columns = listOf("Brand", "Price"),
                    data = listOf(
                        listOf("Honda Civic", 22000),
                        listOf("Toyota Corolla", 25000),
                        listOf("Ford Focus", 27000),
                        listOf("Audi A4", 35000)
                    )
                )
            )
            client.track(nextHourRequest)

            val profileResponse = client.writeProfiles()
            val (profile1, profile2) = client.loadProfiles(profileResponse.profilePaths)

            Assertions.assertEquals(2, profileResponse.profilePaths.size, "number of profiles paths returned")
            Assertions.assertEquals(2, profileResponse.profilesWritten, "number of profiles written")

            // Profile 1 assertions
            Assertions.assertEquals(
                Instant.ofEpochMilli(datasetTimestamp).truncatedTo(ChronoUnit.HOURS),
                profile1.dataTimestamp,
                "dataset timestamp of the written profile"
            )
            Assertions.assertEquals(setOf("Brand", "Price", "Model"), profile1.columns.keys, "The columns from the first two data points")
            Assertions.assertEquals(mapOf("whylogs.tag.tag1" to "value1", "datasetId" to "foo", "orgId" to "nothing"), profile1.tags, "tags")

            // Profile 2 assertions
            Assertions.assertEquals(
                Instant.ofEpochMilli(datasetTimestamp).plus(1, ChronoUnit.HOURS).truncatedTo(ChronoUnit.HOURS),
                profile2.dataTimestamp,
                "dataset timestamp of the written profile"
            )
            Assertions.assertEquals(setOf("Brand", "Price"), profile2.columns.keys, "The columns that were found in the profile2")
            Assertions.assertEquals(mapOf("datasetId" to "foo", "orgId" to "nothing"), profile2.tags, "tags")

            val noOpWrite = client.writeProfiles()
            Assertions.assertTrue(noOpWrite.profilePaths.isEmpty())
            Assertions.assertEquals(0, noOpWrite.profilesWritten)
        }
    }

    @Test
    fun `tags result in different profiles`() = runBlocking {
        client.withServer {
            val datasetTimestamp = 1648751959098

            val firstRequest = LogRequest(
                datasetId = "foo",
                timestamp = datasetTimestamp,
                tags = mapOf("city" to "new york"),
                single = null,
                multiple = MultiLog(
                    columns = listOf("Brand", "Price"),
                    data = listOf(
                        listOf("Honda Civic", 22000),
                        listOf("Toyota Corolla", 25000),
                        listOf("Ford Focus", 27000),
                        listOf("Audi A4", 35000)
                    )
                )
            )
            client.track(firstRequest)

            val secondDataPoint = LogRequest(
                datasetId = "foo",
                timestamp = datasetTimestamp,
                single = null,
                tags = mapOf("city" to "seattle"),
                multiple = MultiLog(
                    columns = listOf("Model", "Price"),
                    data = listOf(
                        listOf("Ford Focus", 27000),
                        listOf("Audi A4", 35000)
                    )
                )
            )
            client.track(secondDataPoint)

            val profileResponse = client.writeProfiles()
            val (profile1, profile2) = client.loadProfiles(profileResponse.profilePaths)

            Assertions.assertEquals(2, profileResponse.profilesWritten, "number of profiles written")
            Assertions.assertEquals(2, profileResponse.profilePaths.size, "number of profiles paths returned")

            // Profile 1 assertions
            Assertions.assertEquals(
                Instant.ofEpochMilli(datasetTimestamp).truncatedTo(ChronoUnit.HOURS),
                profile1.dataTimestamp,
                "dataset timestamp of the written profile"
            )
            Assertions.assertEquals(setOf("Brand", "Price"), profile1.columns.keys, "The columns that were found in profile1")
            Assertions.assertEquals(mapOf("whylogs.tag.city" to "new york", "datasetId" to "foo", "orgId" to "nothing"), profile1.tags, "tags")

            // Profile 2 assertions
            Assertions.assertEquals(
                Instant.ofEpochMilli(datasetTimestamp).truncatedTo(ChronoUnit.HOURS),
                profile2.dataTimestamp,
                "dataset timestamp of the written profile"
            )
            Assertions.assertEquals(setOf("Model", "Price"), profile2.columns.keys, "The columns that were found in the profile2")
            Assertions.assertEquals(mapOf("whylogs.tag.city" to "seattle", "datasetId" to "foo", "orgId" to "nothing"), profile2.tags, "tags")
        }
    }
}

class TestEnvVars : IEnvVars {
    override val writer = WriterTypes.DEBUG_FILE_SYSTEM
    override val whylabsApiEndpoint = "n/a"
    override val orgId = "nothing"
    override val emptyProfilesDatasetIds = emptyList<String>()
    override val requestQueueingMode = WriteLayer.SQLITE
    override val profileStorageMode = WriteLayer.SQLITE
    override val requestQueueProcessingIncrement = PopSize.N(10)
    override val whylabsApiKey = "xxxxxxxxxx.xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
    override val whylogsPeriod = ChronoUnit.HOURS
    override val profileWritePeriod = ProfileWritePeriod.ON_DEMAND
    override val expectedApiKey = "password"
    override val s3Prefix = ""
    override val s3Bucket = "none"
    override val port = 8085
    override val debug = true
}
