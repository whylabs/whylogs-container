package ai.whylabs.services.whylogs.core.config

enum class EnvVarNames(private val default: String? = null) {
    /**
     * A JSON formatted list of string model ids. All of the model
     * ids in this list will have profiles delivered for them regardless
     * of whether or not any data was sent to the container for them. This
     * is useful to differentiate container issues from legitimately not
     * receiving data.
     */
    // container.empty_dataset_ids
    EMPTY_PROFILE_DATASET_IDS("[]"),

    /**
     * A string password that the container requires for each request in
     * the `X-API-Key` header. This is a safeguard if you need to have
     * the container exposed to the internet or other untrusted sources.
     */
    // container.api_key
    CONTAINER_API_KEY,

    FILE_SYSTEM_WRITER_ROOT("whylogs-profiles"),

    /**
     * A JSON list of keys that should be ignored in Kafka data messages.
     */
    IGNORED_KEYS("[]"),

    //
    // Profile upload stuff
    //
    /**
     * Used to determine where profiles are uploaded to.
     * Must be a string value of [WriterTypes]. Other config values become
     * required depending on the value of this.
     */
    // upload.destination
    UPLOAD_DESTINATION(WriterTypes.WHYLABS.name),

    /**
     * The prefix to use for s3 uploads. Only  applies if configured
     * to upload to s3.
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
     */
    // upload.whylabs.api_endpoint
    WHYLABS_API_ENDPOINT("https://api.whylabsapp.com"),

    /**
     * The id of your WhyLabs org. Only applies if configured for
     * uploads to WhyLabs.
     */
    // upload.whylabs.org_id
    ORG_ID,

    /**
     * A WhyLabs api key for your account.
     * Only applies if configured for uploads to WhyLabs.
     */
    // upload.whylabs.api_key
    WHYLABS_API_KEY,

    /**
     * The period to use for log rotation. This can be HOURLY, DAILY.
     * This determines how data is grouped into profiles.
     */
    // whylogs.period
    WHYLOGS_PERIOD,

    PROFILE_WRITE_PERIOD,

    // Kafka stuff
    /**
     * A JSON formatted list of host:port servers.
     * See https://kafka.apache.org/documentation/#consumerconfigs_bootstrap.servers
     * Example: ["http://localhost:9093"]
     */
    // kafka.bootstrap_servers
    KAFKA_BOOTSTRAP_SERVERS,

    /**
     * See https://kafka.apache.org/documentation/#consumerconfigs_group.id
     */
    // kafka.group_id
    KAFKA_GROUP_ID,

    /**
     * Set this to true if you want the container to use its kafka config.
     */
    // kafka.enabled
    KAFKA_ENABLED("false"),

    /**
     * A JSON formatted list of topic names to consume.
     */
    // kafka.topics
    KAFKA_TOPICS("[]"),

    /**
     * A JSON map that maps kafka topic to a whylabs dataset id.
     */
    // kafka.dataset_ids
    KAFKA_TOPIC_DATASET_IDS("{}"),

    /**
     * How to treat nested values in Kafka data messages. Will either include nested values by
     * concatenating keys with "." or it will ignore the entire value.
     */
    KAFKA_MESSAGE_NESTING_BEHAVIOR(KafkaNestingBehavior.Flatten.name),

    /**
     * Number of consumer threads to start up. If you dedicate 3 threads to
     * consumers then there will be three separate threads dedicated to three consumers
     * that independently poll Kafka. If you want to dedicate an entire container to a
     * single consumer then you would put this value to 1.
     */
    KAFKA_CONSUMER_THREADS("1"),

    // REST stuff
    /**
     * The request queueing backend to use for the REST service.
     * This is a throughput optimization to decouple some processing we need to
     * do to requests from the total request time for callers. Should be one of
     * [RequestQueueingMode]. Choosing [RequestQueueingMode.SQLITE] will be more
     * durable at the cost of performance when compared with [RequestQueueingMode.IN_MEMORY].
     * You should only consider changing this if you need more throughput and you
     * don't want to scale the container horizontally.
     */
    // rest.queueing_mode
    REQUEST_QUEUEING_MODE(WriteLayer.SQLITE.name),

    PROFILE_STORAGE_MODE(WriteLayer.SQLITE.name),

    // rest.request_queueing_enabled
    REQUEST_QUEUEING_ENABLED("false"),

    /**
     * Port to use for the rest service.
     */
    // rest.port
    PORT("8080"),

    // Dev stuff
    /**
     * Dev option to enable verbose logging.
     * TODO make this work
     */
    // dev.debug
    DEBUG("false"),

    // Reserved, not yet exposed
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
