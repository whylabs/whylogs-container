package ai.whylabs.services.whylogs.core

import com.whylogs.core.DatasetProfile
import org.slf4j.LoggerFactory
import java.nio.file.Files

interface Writer {
    fun write(profile: DatasetProfile, outputFileName: String, orgId: String, datasetId: String)
}

class SongbirdWriter : Writer {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val songbirdClientManager = SongbirdClientManager()

    override fun write(profile: DatasetProfile, outputFileName: String, orgId: String, datasetId: String) {
        // TODO seems wasteful to have to log to disk first. They're ready to go as-is. Might need a new songbird API.
        val tempFile = Files.createTempFile("whylogs", "profile")
        logger.debug("Write profile to temp path: {}", tempFile.toAbsolutePath())
        try {
            logger.info("Sending $outputFileName to whylabs $orgId")
            songbirdClientManager.logApi.log(
                orgId,
                datasetId,
                profile.dataTimestamp.epochSecond,
                emptyList(),
                tempFile.toFile()
            )
        } finally {
            logger.debug("Clean up temp file: {}", tempFile)
            Files.deleteIfExists(tempFile)
        }
    }
}
