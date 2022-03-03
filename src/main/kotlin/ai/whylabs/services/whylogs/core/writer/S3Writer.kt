package ai.whylabs.services.whylogs.core.writer

import ai.whylabs.services.whylogs.core.config.EnvVars
import ai.whylabs.services.whylogs.core.config.IEnvVars
import ai.whylabs.services.whylogs.core.randomAlphaNumericId
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.DefaultAwsRegionProviderChain
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ObjectMetadata
import com.github.michaelbull.retry.retry
import com.whylogs.core.DatasetProfile
import org.slf4j.LoggerFactory
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.Locale

class S3Writer(private val envVars: IEnvVars = EnvVars.instance) : Writer {
    private val logger = LoggerFactory.getLogger(javaClass)

    private val keyFormatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneId.from(ZoneOffset.UTC))
    private var dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        .withLocale(Locale.US)
        .withZone(ZoneId.from(ZoneOffset.UTC))

    private val s3Client = AmazonS3ClientBuilder.standard()
        .withRegion(DefaultAwsRegionProviderChain().region)
        .withCredentials(DefaultAWSCredentialsProviderChain())
        .build()

    override suspend fun write(profile: DatasetProfile, orgId: String, datasetId: String): WriteResult {
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
            return WriteResult("s3", "s3://${envVars.s3Bucket}/$key")
        } catch (t: Throwable) {
            logger.error("Failed to upload profile to s3", t)
            throw t // TODO why didn't I throw these before?
        }
    }
}
