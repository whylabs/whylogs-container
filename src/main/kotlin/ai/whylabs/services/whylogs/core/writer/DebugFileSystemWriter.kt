package ai.whylabs.services.whylogs.core.writer

import ai.whylabs.services.whylogs.core.config.EnvVars
import ai.whylabs.services.whylogs.core.config.IEnvVars
import ai.whylabs.services.whylogs.core.randomAlphaNumericId
import com.github.michaelbull.retry.retry
import com.whylogs.core.DatasetProfile
import org.slf4j.LoggerFactory
import java.io.File
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.util.Locale

class DebugFileSystemWriter(private val envVars: IEnvVars = EnvVars.instance) : Writer {
    private val logger = LoggerFactory.getLogger(javaClass)

    private var dayFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
        .withLocale(Locale.US)
        .withZone(ZoneId.from(ZoneOffset.UTC))

    override suspend fun write(profile: DatasetProfile, orgId: String, datasetId: String): WriteResult {
        val tags = parseTags(profile, removePrefixes = false)
        val tagString = getTagString(tags)

        val randomId = randomAlphaNumericId() // TODO maybe use the session id in here?
        val isoDate = profile.dataTimestamp.toString().let {
            // If we're on windows then we have to replace the : with _ in the timestamps in file names
            // because windows apparently can't handle that.
            if (System.getProperty("os.name").toLowerCase().contains("windows")) {
                it.replace(":", "_")
            } else {
                it
            }
        }

        val dayDate = dayFormatter.format(profile.dataTimestamp)

        val proto = profile.toProtobuf().build()
        val bytes = proto.toByteArray()
        val prefix = envVars.fileSystemWriterRoot

        val pwd = System.getProperty("user.dir")
        val filePath = "$pwd/$prefix/$dayDate"
        val fileName = "$filePath/${randomId}_$isoDate.bin"

        try {
            retry(retryPolicy) {
                File(filePath).apply { mkdirs() }
                File(fileName).apply {
                    writeBytes(bytes)
                }
            }
            logger.info("Wrote profile ${profile.sessionId} with tags $tagString to file $fileName")
            return WriteResult("file", fileName)
        } catch (t: Throwable) {
            logger.error("Failed to write to disk", t)
            throw t
        }
    }
}
