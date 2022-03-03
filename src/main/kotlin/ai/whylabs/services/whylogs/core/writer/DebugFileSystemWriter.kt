package ai.whylabs.services.whylogs.core.writer

import ai.whylabs.services.whylogs.core.config.EnvVars
import ai.whylabs.services.whylogs.core.config.IEnvVars
import ai.whylabs.services.whylogs.core.randomAlphaNumericId
import com.github.michaelbull.retry.retry
import com.whylogs.core.DatasetProfile
import java.io.File
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.Locale
import org.slf4j.LoggerFactory

// TODO get the file system prefix from env vars
class DebugFileSystemWriter(private val envVars: IEnvVars = EnvVars.instance) : Writer {
    private val logger = LoggerFactory.getLogger(javaClass)

    private val keyFormatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneId.from(ZoneOffset.UTC))
    private var dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        .withLocale(Locale.US)
        .withZone(ZoneId.from(ZoneOffset.UTC))

    override suspend fun write(profile: DatasetProfile, orgId: String, datasetId: String): WriteResult {
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
            return WriteResult("file", "$pwd/$fileName")
        } catch (t: Throwable) {
            logger.error("Failed to write to disk", t)
            throw t
        }
    }
}
