package ai.whylabs.services.whylogs.core.writer

import ai.whylabs.service.invoker.ApiException
import ai.whylabs.service.model.LogAsyncRequest
import ai.whylabs.services.whylogs.core.DatasetIdTag
import ai.whylabs.services.whylogs.core.SongbirdClientManager
import ai.whylabs.services.whylogs.core.config.EnvVars
import ai.whylabs.services.whylogs.core.config.IEnvVars
import com.github.michaelbull.retry.retry
import com.whylogs.core.DatasetProfile
import org.slf4j.LoggerFactory
import java.net.HttpURLConnection
import java.net.URL

private const val writeType = "whylabs"

class SongbirdWriter(envVars: IEnvVars = EnvVars.instance) : Writer {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val songbirdClientManager = SongbirdClientManager(envVars)


    override suspend fun write(profile: DatasetProfile, orgId: String, datasetId: String): WriteResult {
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
            return WriteResult(type = writeType)
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
