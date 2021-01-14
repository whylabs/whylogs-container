package ai.whylabs.services.whylogs.core

import java.nio.channels.FileChannel
import java.nio.file.Files
import ai.whylabs.songbird.model.SegmentTag
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.whylogs.core.DatasetProfile
import org.slf4j.LoggerFactory

interface Writer {
    fun write(profile: DatasetProfile, outputFileName: String, orgId: String, datasetId: String)
}

class SongbirdWriter : Writer {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val songbirdClientManager = SongbirdClientManager()
    private val mapper = jacksonObjectMapper()

    override fun write(profile: DatasetProfile, outputFileName: String, orgId: String, datasetId: String) {
        // TODO seems wasteful to have to log to disk first. They're ready to go as-is. Might need a new songbird API.
        val tempFile = Files.createTempFile("whylogs", "profile")
        logger.debug("Write profile to temp path: {}", tempFile.toAbsolutePath())

        val tags = profile.tags
            .filterKeys { it.startsWith(SegmentTagPrefix) }
            .map { tag ->
                SegmentTag().apply {
                    key = tag.key.substring(SegmentTagPrefix.length)
                    value = tag.value
                }
            }

        try {
            Files.newOutputStream(tempFile).use {
                profile.toProtobuf().build().writeDelimitedTo(it)
            }

            val size = FileChannel.open(tempFile).use { it.size() }
            val tagString = tags.joinToString(",") { "[${it.key}=${it.value}]" }
            logger.info("Pushing ${profile.tags[DatasetIdTag]}/$tagString/${profile.dataTimestamp} to WhyLabs $orgId. Size: $size bytes")
            songbirdClientManager.logApi.log(
                orgId,
                datasetId,
                profile.dataTimestamp.toEpochMilli(),
                emptyList(),
                if (tags.isEmpty()) null else mapper.writeValueAsString(tags),
                tempFile.toFile()
            )
            logger.info("Pushed ${profile.tags[DatasetIdTag]}/${tagString}/${profile.dataTimestamp} data successfully")

        } finally {
            logger.debug("Clean up temp file: {}", tempFile)
            Files.deleteIfExists(tempFile)
        }
    }
}
