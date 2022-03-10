package ai.whylabs.services.whylogs.core.config

import org.slf4j.LoggerFactory

private val log = LoggerFactory.getLogger(KafkaConfig::class.java)

enum class KafkaNestingBehavior {
    Ignore,
    Flatten
}

data class KafkaConfig(
    val consumerCount: Int,
    val topics: List<String>,
    val datasetIds: Map<String, String>, // Only applies if using the whylabs writer
    val bootstrapServers: List<String>,
    val groupId: String,
    val nestingBehavior: KafkaNestingBehavior
) {
    companion object {

        fun parse(parent: IEnvVars): KafkaConfig? {
            val enabled = EnvVarNames.KAFKA_ENABLED.getOrDefault().toBoolean()

            if (!enabled) {
                log.warn("Not using kafka config because ${EnvVarNames.KAFKA_ENABLED.name} is $enabled. Should be true.")
                return null
            }

            val topics = parseEnvList(EnvVarNames.KAFKA_TOPICS)
            val datasetIds = parseEnvMap(EnvVarNames.KAFKA_TOPIC_DATASET_IDS.requireIf(parent.writer == WriterTypes.WHYLABS))
            val nestingBehavior = KafkaNestingBehavior.valueOf(EnvVarNames.KAFKA_MESSAGE_NESTING_BEHAVIOR.getOrDefault())

            if (parent.writer == WriterTypes.WHYLABS && !datasetIds.keys.containsAll(topics.toSet())) {
                val keyDiff = topics.toSet() subtract datasetIds.keys
                throw IllegalStateException("Dataset ids found in config ${EnvVarNames.KAFKA_TOPICS} that weren't present in ${EnvVarNames.KAFKA_TOPIC_DATASET_IDS.name}: $keyDiff")
            }

            return KafkaConfig(
                consumerCount = EnvVarNames.KAFKA_CONSUMER_THREADS.getOrDefault().toInt(),
                topics = parseEnvList(EnvVarNames.KAFKA_TOPICS),
                bootstrapServers = parseEnvList(EnvVarNames.KAFKA_BOOTSTRAP_SERVERS),
                nestingBehavior = nestingBehavior,
                groupId = EnvVarNames.KAFKA_GROUP_ID.require(),
                datasetIds = parseEnvMap(EnvVarNames.KAFKA_TOPIC_DATASET_IDS.requireIf(parent.writer == WriterTypes.WHYLABS))
            )
        }
    }
}
