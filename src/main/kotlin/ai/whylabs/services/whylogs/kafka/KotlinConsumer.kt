package ai.whylabs.services.whylogs.kafka

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.slf4j.LoggerFactory
import java.time.Duration
import kotlin.coroutines.CoroutineContext

private val log = LoggerFactory.getLogger(KotlinConsumer::class.java)

private fun <K, V> getConsumer(config: KotlinConsumerConfig<K, V>): KafkaConsumer<String, String> {
    val consumerConfig = mapOf(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to config.bootstrapServers,
        ConsumerConfig.GROUP_ID_CONFIG to config.groupId,
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to "org.apache.kafka.common.serialization.StringDeserializer",
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to "org.apache.kafka.common.serialization.StringDeserializer",
    )

    log.info("Setting up kafka consumer with config $consumerConfig")
    val consumer = KafkaConsumer<String, String>(consumerConfig)
    consumer.subscribe(config.topics)
    return consumer
}

data class KotlinConsumerConfig<K, V>(
    val consumerCount: Int = 1,
    val topics: List<String>,
    val bootstrapServers: List<String>,
    val groupId: String,
)

private val mapper = jacksonObjectMapper()

data class KafkaRecord<K, V>(
    val value: V,
    val key: K?,
    val rawRecord: ConsumerRecord<String, String>
)

class KotlinConsumer<K, V>(
    private val config: KotlinConsumerConfig<K, V>,
    private val valueType: TypeReference<V>,
    private val keyType: TypeReference<K>,
    private val process: suspend (record: KafkaRecord<K, V>) -> Boolean
) :
    CoroutineScope {

    companion object {
        inline fun <reified K, reified V> consumer(
            config: KotlinConsumerConfig<K, V>,
            noinline processor: suspend (record: KafkaRecord<K, V>) -> Boolean
        ): KotlinConsumer<K, V> {
            return KotlinConsumer(config, object : TypeReference<V>() {}, object : TypeReference<K>() {}, processor)
        }
    }

    init {
        Runtime.getRuntime().addShutdownHook(Thread { stop() })
    }

    private val supervisorJob = SupervisorJob()
    override val coroutineContext: CoroutineContext
        get() = Dispatchers.IO + supervisorJob

    fun stop() {
        supervisorJob.cancel()
    }

    fun start() = launch {
        log.info("Creating ${config.consumerCount} consumers with dedicated threads.")
        repeat(config.consumerCount) {
            launchWorker()
        }
    }

    private fun CoroutineScope.launchWorker() = launch {
        val consumer = getConsumer<K, V>(config)

        repeatUntilCancelled(log) {
            log.debug("Polling for messages")
            val records = consumer.poll(Duration.ofSeconds(10))

            if (records.count() == 0) {
                // do nothing
                log.debug("Got no records to process.")
            } else {
                log.info("Got ${records.count()} records to process.")
                records.forEach forEach@{ record ->
                    // TODO Add a config option for a failure topic. If we can't handle a message here then we'll just drop it. We
                    // could at least forward that to a failure topic so we can find out why it can't be tracked with whylogs.
                    val valueKey: String? = record.key()
                    val valueString: String = record.value()
                    val kafkaRecord = try {
                        val parsedKey: K? = valueKey?.let { mapper.readValue(valueKey, keyType) }
                        val parsedRecord: V = mapper.readValue(valueString, valueType) // TODO is there a built in method of deserializing?
                        KafkaRecord(parsedRecord, parsedKey, record)
                    } catch (t: Throwable) {
                        log.error("Error deserializing the kafka record.", t)
                        return@forEach
                    }

                    try {
                        process(kafkaRecord)
                    } catch (t: Throwable) {
                        log.error("Error processing kafka record $valueKey $valueString, dropping permanently.", t)
                    }
                }
            }
        }
    }
}
