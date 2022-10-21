package ai.whylabs.services.whylogs.core.config

import java.time.temporal.ChronoUnit

/**
 * @property default The default value.
 */
enum class EnvVarNames(val default: String? = null) {
    /**
     * A JSON formatted list of string model ids. All the model
     * ids in this list will have profiles delivered for them regardless
     * of whether any data was sent to the container for them. This
     * is useful to differentiate container issues from legitimately not
     * receiving data.
     *
     * Defaults to `[]`
     */
    // container.empty_dataset_ids
    EMPTY_PROFILE_DATASET_IDS("[]"),

    /**
     * Disables the password auth header that the container uses to validate
     * requests. Some integration paths make it difficult to send custom headers
     * values and VPC privacy may be adequate for use cases as a substitute.
     */
    // container.disable_auth
    DISABLE_AUTH("false"),

    /**
     * A string password that the container requires for each request in
     * the `X-API-Key` header. This is a safeguard if you need to have
     * the container exposed to the internet or other untrusted sources.
     *
     * Required.
     */
    // container.api_key
    CONTAINER_API_KEY,

    /**
     * Controls the data structure that stores the profiles before they get
     * uploaded. By default, [WriteLayer.IN_MEMORY] will use an in memory map.
     *
     * Defaults to `IN_MEMORY`. One of [WriteLayer].
     */
    // container.profile_storage_mode
    PROFILE_STORAGE_MODE(WriteLayer.IN_MEMORY.name),

    /**
     * Only applies when [UPLOAD_DESTINATION] is set to [WriterTypes.DEBUG_FILE_SYSTEM].
     * Controls the dir that whylogs profiles are written to, relative to the local dir.
     *
     * Defaults to `whylogs-profiles`
     */
    // container.file_system_writer_root
    FILE_SYSTEM_WRITER_ROOT("whylogs-profiles"),

    /**
     * A JSON list of keys that should be ignored when logging. If any of the columns of data messages
     * match these keys then they won't be logged. Useful to avoid having to strip out data as
     * a preprocessing step. If the container is running in Kafka mode then you can use this to avoid having
     * to strip keys out of messages. If you're using the REST interface then any of the columns in the single
     * or multiple will be dropped if they match any of the ones in here.
     *
     * Defaults to `[]`
     */
    // container.ignored_keys
    IGNORED_KEYS("[]"),

    //
    // Profile upload stuff
    //

    /**
     * How frequent the container should upload profiles. This defaults to the same cadence as the
     * model definition. For an hourly model, you'll upload profiles on an hourly basis. If this is set
     * to [ProfileWritePeriod.HOURS] then you'll upload profiles every hour.
     *
     * Defaults to whatever [WHYLOGS_PERIOD] is. One of [ProfileWritePeriod]
     */
    // upload.write_period
    PROFILE_WRITE_PERIOD,

    /**
     * Used to determine where profiles are uploaded to.
     * Must be a string value of [WriterTypes]. Other config values become
     * required depending on the value of this.
     *
     * - [WriterTypes.WHYLABS] causes the container to upload profiles to whylabs.
     * - [WriterTypes.S3] causes the container to upload profiles to s3.
     * - [WriterTypes.DEBUG_FILE_SYSTEM] causes the container to upload profiles to disk.
     *   This was developed mostly as a debugging tool.
     *
     * Defaults to `WHYLABS`. One of [WriterTypes].
     */
    // upload.destination
    UPLOAD_DESTINATION(WriterTypes.WHYLABS.name),

    /**
     * The prefix to use for s3 uploads. Only  applies if configured
     * to upload to s3.
     *
     * Defaults to `""`
     */
    // upload.s3.prefix
    S3_PREFIX(""),

    /**
     * The s3 bucket to use for profile uploads. Only applies if
     * configured to upload to s3.
     */
    // upload.s3.bucket
    S3_BUCKET,

    //
    // WhyLabs stuff
    //
    /**
     * A URL to use to upload profiles to. Useful for debugging or potentially
     * standing up a WhyLabs compatible endpoint that you can customize. We need
     * to document how to do that.
     * Only applies if configured for uploads to WhyLabs.
     *
     * Defaults to `https://api.whylabsapp.com`
     */
    // upload.whylabs.api_endpoint
    WHYLABS_API_ENDPOINT("https://api.whylabsapp.com"),

    /**
     * The id of your WhyLabs org. Only applies if configured for
     * uploads to WhyLabs.
     *
     * Required if [UPLOAD_DESTINATION] is [WriterTypes.WHYLABS].
     */
    // upload.whylabs.org_id
    ORG_ID,

    /**
     * A WhyLabs api key for your account.
     * Only applies if configured for uploads to WhyLabs.
     *
     * Required if [UPLOAD_DESTINATION] is [WriterTypes.WHYLABS].
     */
    // upload.whylabs.api_key
    WHYLABS_API_KEY,

    /**
     * The period to use for log rotation. This can be HOURLY, DAILY.
     * This determines how data is grouped into profiles. If you're using
     * WhyLabs then this should match the model's type.
     *
     * Required. One of [ChronoUnit.HOURS] or [ChronoUnit.DAYS].
     */
    // whylogs.period
    WHYLOGS_PERIOD,

    // Kafka stuff
    /**
     * A JSON formatted list of host:port servers.
     * See __[Kafka Bootstrap Servers](https://kafka.apache.org/documentation/#consumerconfigs_group.id)__
     * Example: `["http://localhost:9092"]`
     *
     * Required when [KAFKA_ENABLED].
     */
    // kafka.bootstrap_servers
    KAFKA_BOOTSTRAP_SERVERS,

    /**
     * The container will have a single group id for all the consumers.
     * See __[Kafka Group Ids](https://kafka.apache.org/documentation/#consumerconfigs_group.id)__
     *
     * Required when [KAFKA_ENABLED].
     */
    // kafka.group_id
    KAFKA_GROUP_ID,

    /**
     * Set this to true if you want the container to use its kafka config.
     * By default, none of the Kafka options do anything.
     *
     * Defaults to `false`
     */
    // kafka.enabled
    KAFKA_ENABLED("false"),

    /**
     * A JSON formatted list of topic names to consume. The full list is
     * subscribed to by each of the consumers. [EnvVarNames.KAFKA_TOPICS.default] huh
     *
     * Defaults to `[]`
     */
    // kafka.topics
    KAFKA_TOPICS("[]"),

    /**
     * A JSON map that maps kafka topics to a whylabs dataset id. Applies when
     * [UPLOAD_DESTINATION] is set to [WriterTypes.WHYLABS].
     *
     * Defaults to `{}`
     */
    // kafka.dataset_ids
    KAFKA_TOPIC_DATASET_IDS("{}"),

    /**
     * How to treat nested values in Kafka JSON data messages. Will either include nested values by
     * concatenating keys with "." or it will ignore the entire value.
     *
     * Defaults to `FLATTEN`. One of [KafkaNestingBehavior].
     */
    // kafka.message_nesting_behavior
    KAFKA_MESSAGE_NESTING_BEHAVIOR(KafkaNestingBehavior.FLATTEN.name),

    /**
     * Number of consumer threads to start up. If you dedicate 3 threads to
     * consumers then there will be three separate threads dedicated to three consumers
     * that independently poll Kafka. If you want to dedicate an entire container to a
     * single consumer then you would put this value to 1. The ideal value depends on
     * use case, Kafka cluster configuration, and the hardware used to host the
     * container. Having a thread count == your topic partition count is reasonable.
     * If you have more threads than your partitions then the extra ones will
     * just be idle.
     *
     * Just keep in mind that messages in a topic partition are FIFO, so you won't get
     * benefit if your topic only has a single partition.
     *
     * Defaults to `1`
     */
    // kafka.consumer_threads
    KAFKA_CONSUMER_THREADS("1"),

    // REST stuff
    /**
     * Only applies if [REQUEST_QUEUEING_ENABLED] is `true`.
     * That queue can be backed by in memory data structures or sqlite.
     * See [REQUEST_QUEUEING_ENABLED] for more info.
     *
     * Defaults to `IN_MEMORY`. One of [WriteLayer].
     */
    // rest.queueing_mode
    REQUEST_QUEUEING_MODE(WriteLayer.IN_MEMORY.name),

    /**
     * An optimization that decouples the request from the request handling.
     * This will make each request finish faster from the caller's perspective
     * by queueing the requests to be handled asap, rather than handling them
     * while the caller waits. This isn't that useful when using the default
     * `PROFILE_STORAGE_MODE=IN_MEMORY` since the request handling is pretty fast
     * already. It was added to make `PROFILE_STORAGE_MODE=SQLITE` faster since
     * there is af air bit of IO for each request. You probably don't need to
     * change this.
     *
     * Defaults to `false`
     */
    // rest.request_queueing_enabled
    REQUEST_QUEUEING_ENABLED("false"),

    /**
     * Port to use for the REST service.
     *
     * Defaults to `8080`
     */
    // rest.port
    PORT("8080"),

    // Dev stuff
//    /**
//     * Dev option to enable verbose logging.
//     * TODO make this work
//     */
//    // dev.debug
//    DEBUG("false"),

    /**
     * A fairly obscure/advanced tuning knob that only applies to the REST
     * calls when [REQUEST_QUEUEING_ENABLED] is `true`. You probably don't
     * want to set this.
     */
    // rest.request_queue_processing_increment
    REQUEST_QUEUE_PROCESSING_INCREMENT;

    fun get(): String? {
        return System.getenv(name)
    }

    fun getOrDefault(): String {
        return System.getenv(name) ?: default ?: throw IllegalStateException("No default defined for $name")
    }

    fun requireIf(condition: Boolean, fallback: String = default ?: ""): String {
        val value = System.getenv(name)

        if (value.isNullOrEmpty() && condition) {
            throw java.lang.IllegalArgumentException("Must supply env var $name")
        }

        return value ?: fallback
    }

    fun require(fallback: String = default ?: "") = requireIf(true, fallback)
}
