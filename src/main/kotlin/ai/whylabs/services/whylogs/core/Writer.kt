package ai.whylabs.services.whylogs.core

import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import com.amazonaws.AmazonServiceException
import com.amazonaws.SdkClientException
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.AmazonS3URI
import com.amazonaws.services.s3.model.PutObjectRequest
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams
import com.whylogs.core.DatasetProfile
import org.slf4j.LoggerFactory

interface Writer {
    fun write(profile: DatasetProfile, outputFileName: String)
}

class S3Writer(s3OutputPath: String, private val awsKmsKeyId: String?) : Writer {
    private val logger = LoggerFactory.getLogger(javaClass)

    private val s3Uri: AmazonS3URI
    private val s3: AmazonS3

    init {
        s3 = AmazonS3ClientBuilder.standard()
            .build()
        val pathWithouSlash = s3OutputPath.replace(Regex("/$"), "")
        s3Uri = AmazonS3URI(pathWithouSlash)

        logger.info("Using S3 buket: {}. Prefix: {}", s3Uri.bucket, s3Uri.key)
    }

    override fun write(profile: DatasetProfile, outputFileName: String) {
        val tempFile = Files.createTempFile("whylogs", "profile")
        logger.debug("Write profile to temp path: {}", tempFile.toAbsolutePath())
        try {
            Files.newOutputStream(tempFile,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE).use { os ->
                profile.toProtobuf().build().writeDelimitedTo(os)
            }

            val prefix = s3Uri.key
            val s3Path = if (prefix == null) {
                outputFileName
            } else {
                "$prefix/$outputFileName"
            }
            val putRequest = PutObjectRequest(s3Uri.bucket, s3Path, tempFile.toFile())
            if (awsKmsKeyId != null) {
                logger.debug("Using AWS KMS Key ID for SSE: {}", awsKmsKeyId)
                putRequest.sseAwsKeyManagementParams = SSEAwsKeyManagementParams(awsKmsKeyId)
            }
            s3.putObject(putRequest)
            logger.info("Data uploaded to S3: s3://${s3Uri.bucket}/$s3Path")

        } catch (e: IOException) {
            logger.warn("Failed to write output to path: {}", tempFile, e)
        } catch (e: SdkClientException) {
            logger.warn("Failed to upload data to S3. Path: {}{}", s3Uri, outputFileName, e)
        } catch (e: AmazonServiceException) {
            logger.warn("S3 service returns an exception for upload. Path: {}", s3Uri, e)
        } finally {
            logger.debug("Clean up temp file: {}", tempFile)
            Files.deleteIfExists(tempFile)
        }
    }

}

class LocalWriter(outputPath: String) : Writer {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val path: Path = Paths.get(outputPath)

    init {
        try {
            Files.createDirectories(path)
        } catch (e: IOException) {
            logger.error("Failed to create/access output path at: {}", path)
            throw e
        }
    }

    override fun write(profile: DatasetProfile, outputFileName: String) {
        val outputFile = path.resolve(outputFileName)
        logger.debug("Write profile to: {}", outputFile.toAbsolutePath())
        try {
            Files.newOutputStream(outputFile,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE).use { os ->
                profile.toProtobuf().build().writeDelimitedTo(os)
            }
            logger.debug("Wrote output to: {}", outputFile.toAbsolutePath())
        } catch (e: IOException) {
            logger.warn("Failed to write output to path: {}", outputFile, e)
        }
    }
}