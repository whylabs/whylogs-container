package ai.whylabs.services.whylogs.core.writer

import ai.whylabs.services.whylogs.core.EnvVars
import ai.whylabs.services.whylogs.core.IEnvVars
import ai.whylabs.services.whylogs.core.randomAlphaNumericId
import com.github.michaelbull.retry.retry
import com.whylogs.core.DatasetProfile
import org.slf4j.LoggerFactory
import java.io.File
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.Locale

// TODO get the file system prefix from env vars
class DebugFileSystemWriter(private val envVars: IEnvVars = EnvVars()) : Writer {
    private val logger = LoggerFactory.getLogger(javaClass)

    private val keyFormatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneId.from(ZoneOffset.UTC))
    private var dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        .withLocale(Locale.US)
        .withZone(ZoneId.from(ZoneOffset.UTC))

    override suspend fun write(profile: DatasetProfile, orgId: String, datasetId: String): String {
        val tags = parseTags(profile, removePrefixes = false)
        val tagString = getTagString(tags)

        val randomId = randomAlphaNumericId() // TODO maybe use the session id in here?
        val isoDate = keyFormatter.format(profile.dataTimestamp)
        val keyDate = dateFormatter.format(profile.dataTimestamp)

        val proto = profile.toProtobuf().build()
        val bytes = proto.toByteArray()
        val prefix = envVars.fileSystemWriterRoot

        val filePath = "$prefix/$keyDate"
        val fileName = "$filePath/${randomId}_$isoDate.bin"

        try {
            retry(retryPolicy) {
                File(filePath).apply { mkdirs() }
                File(fileName).apply { writeBytes(bytes) }
            }
            logger.info("Wrote profile ${profile.sessionId} with tags $tagString to file $fileName")
            val pwd = System.getProperty("user.dir")
            return "$pwd/$fileName"
        } catch (t: Throwable) {
            logger.error("Failed to write to disk", t)
            throw t
        }
    }
}
