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
        val tempFile = Files.createTempFile("whylogs", "profile")
        logger.debug("Write profile to temp path: {}", tempFile.toAbsolutePath())
        try {
            logger.info("Sending $outputFileName to whylabs")
            songbirdClientManager.logApi.log(
                orgId,
                datasetId,
                profile.dataTimestamp.epochSecond,
                emptyList(),
                tempFile.toFile()
            )

        } catch (e: Exception) {
            logger.warn("Failed to write to whylabs {}", tempFile, e)
        } finally {
            logger.debug("Clean up temp file: {}", tempFile)
            Files.deleteIfExists(tempFile)
        }
    }
}
