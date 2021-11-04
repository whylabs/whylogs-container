package ai.whylabs.services.whylogs.core

import ai.whylabs.service.invoker.ApiException
import ai.whylabs.service.model.LogAsyncRequest
import ai.whylabs.service.model.SegmentTag
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.DefaultAwsRegionProviderChain
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ObjectMetadata
import com.github.michaelbull.retry.policy.RetryPolicy
import com.github.michaelbull.retry.policy.fullJitterBackoff
import com.github.michaelbull.retry.policy.limitAttempts
import com.github.michaelbull.retry.policy.plus
import com.github.michaelbull.retry.retry
import com.whylogs.core.DatasetProfile
import org.slf4j.LoggerFactory
import java.net.HttpURLConnection
import java.net.URL
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.Locale

interface Writer {
    suspend fun write(profile: DatasetProfile, orgId: String, datasetId: String)

    fun getTagString(tags: List<SegmentTag>): String {
        return if (tags.isEmpty()) "NO_TAGS" else tags.joinToString(",") { "[${it.key}=${it.value}]" }
    }

    /**
     * Get just the user tags that we prefix, without the prefix.
     */
    fun parseTags(profile: DatasetProfile, removePrefixes: Boolean = true): List<SegmentTag> {
        if (removePrefixes) {
            return profile.tags
                .filterKeys { it.startsWith(SegmentTagPrefix) }
                .map { tag ->
                    SegmentTag().apply {
                        key = tag.key.substring(SegmentTagPrefix.length)
                        value = tag.value
                    }
                }
        } else {
            return profile.tags
                .map { tag ->
                    SegmentTag().apply {
                        key = tag.key
                        value = tag.value
                    }
                }
        }
    }
}

private val retryPolicy: RetryPolicy<Throwable> = limitAttempts(3) + fullJitterBackoff(base = 10, max = 5_000)

class S3Writer(private val envVars: IEnvVars = EnvVars()) : Writer {
    private val logger = LoggerFactory.getLogger(javaClass)

    private val keyFormatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneId.from(ZoneOffset.UTC))
    private var dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        .withLocale(Locale.US)
        .withZone(ZoneId.from(ZoneOffset.UTC))

    private val s3Client = AmazonS3ClientBuilder.standard()
        .withRegion(DefaultAwsRegionProviderChain().region)
        .withCredentials(DefaultAWSCredentialsProviderChain())
        .build()

    override suspend fun write(profile: DatasetProfile, orgId: String, datasetId: String) {
        val tags = parseTags(profile, removePrefixes = false) // There won't be prefixes if we're uploading to s3
        val tagString = getTagString(tags)

        val randomId = randomAlphaNumericId()
        val isoDate = keyFormatter.format(profile.dataTimestamp)
        val keyDate = dateFormatter.format(profile.dataTimestamp)
        // Add a unique prefix to each upload to make sure that people uploading from multiple containers won't clobber profiles.
        val prefix = if (envVars.s3Prefix.isEmpty()) "" else "${envVars.s3Prefix}/"
        val key = "${prefix}$keyDate/${randomId}_$isoDate.bin"
        val proto = profile.toProtobuf().build()
        val bytes = proto.toByteArray().inputStream()

        val metadata = ObjectMetadata().apply {
            addUserMetadata("whylogs-dataset-epoch-millis", profile.dataTimestamp.toEpochMilli().toString())
            addUserMetadata("whylogs-session-id", profile.sessionId)
            addUserMetadata("whylogs-dataset-id", datasetId)
            addUserMetadata("whylogs-session-epoch-millis", profile.sessionTimestamp.toEpochMilli().toString())
            addUserMetadata("whylogs-segment-tags", tagString)
        }

        try {
            retry(retryPolicy) {
                s3Client.putObject(envVars.s3Bucket, key, bytes, metadata)
            }
            logger.info("Uploaded profile ${profile.sessionId} with tags $tagString to s3 with key $key")
        } catch (t: Throwable) {
            logger.error("Failed to upload profile to s3", t)
        }
    }
}

class SongbirdWriter : Writer {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val songbirdClientManager = SongbirdClientManager()

    override suspend fun write(profile: DatasetProfile, orgId: String, datasetId: String) {
        val tags = parseTags(profile)

        try {
            val uploadResponse = retry(retryPolicy) {
                songbirdClientManager.logApi.logAsync(
                    orgId,
                    datasetId,
                    LogAsyncRequest()
                        .datasetTimestamp(profile.dataTimestamp.toEpochMilli())
                        .segmentTags(tags)
                )
            }

            retry(retryPolicy) {
                uploadToUrl(uploadResponse.uploadUrl!!, profile)
            }

            val tagString = getTagString(tags)
            logger.info("Pushed ${profile.tags[DatasetIdTag]}/$tagString/${profile.dataTimestamp} data successfully")
        } catch (e: ApiException) {
            logger.error("Bad request when sending data to WhyLabs. Code: ${e.code}. Message: ${e.responseBody}", e)
            throw e
        } catch (t: Throwable) {
            logger.error("Fail to send data to WhyLabs", t)
            throw t
        }
    }
}

private fun uploadToUrl(url: String, profile: DatasetProfile) {
    val connection = URL(url).openConnection() as HttpURLConnection
    connection.doOutput = true
    connection.setRequestProperty("Content-Type", "application/octet-stream")
    connection.requestMethod = "PUT"

    connection.outputStream.use { out ->
        profile.toProtobuf().build().writeTo(out)
    }

    if (connection.responseCode != 200) {
        throw RuntimeException("Error uploading profile: ${connection.responseCode} ${connection.responseMessage}")
    }
}
