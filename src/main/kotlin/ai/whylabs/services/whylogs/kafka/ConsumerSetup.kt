package ai.whylabs.services.whylogs.kafka

import ai.whylabs.services.whylogs.core.LogRequest
import ai.whylabs.services.whylogs.core.WhyLogsProfileManager
import ai.whylabs.services.whylogs.core.config.IEnvVars
import ai.whylabs.services.whylogs.core.config.KafkaConfig
import ai.whylabs.services.whylogs.core.config.WriterTypes
import ai.whylabs.services.whylogs.core.randomAlphaNumericId
import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger(KafkaConfig::class.java)

class ConsumerController(private val envConfig: IEnvVars, private val profileManager: WhyLogsProfileManager) {
    // This class shouldn't be created if kafka isn't configured
    private val kafkaConfig = envConfig.kafkaConfig ?: throw IllegalStateException("Kafka is disabled in the config. Not making consumer.")

    private val consumer: KotlinConsumer<String, Map<String, Any>> = run {
        val config = KotlinConsumerConfig<String, Map<String, Any>>(
            topics = kafkaConfig.topics,
            consumerCount = kafkaConfig.consumerCount,
            bootstrapServers = kafkaConfig.bootstrapServers,
            groupId = kafkaConfig.groupId,
        )
        log.info("Initializing kafka consumer.")
        KotlinConsumer.consumer(config, ::process).apply { start() }
    }

    private suspend fun process(record: KafkaRecord<String, Map<String, Any>>): Boolean {

        val topic = record.rawRecord.topic()
        val datasetId = if (envConfig.writer == WriterTypes.WHYLABS) {
            kafkaConfig.datasetIds[topic] ?: throw IllegalStateException("Don't know which whylabs dataset id to use for topic $topic")
        } else {
            "dataset-${randomAlphaNumericId()}" // some reasonable default. This isn't really important outside whylabs yet.
        }

        val request = LogRequest(
            datasetId = datasetId,
            timestamp = record.rawRecord.timestamp(), // Is this the right place to get it from?
            tags = mapOf(), // TODO implement this after the whylogs API refresh
            single = record.value, // TODO implement nested parsing for the input map
            multiple = null // Need a way to support this mode as well
        )

        return try {
            profileManager.handle(request)
            profileManager.mergePending()
            true
        } catch (t: Throwable) {
            log.error("Error while processing kafka.", t)
            false
        }
    }
}
