package ai.whylabs.services.whylogs.core

import ai.whylabs.services.whylogs.core.config.IEnvVars
import ai.whylabs.services.whylogs.core.config.ProfileWritePeriod
import ai.whylabs.services.whylogs.core.config.WriteLayer
import ai.whylabs.services.whylogs.core.config.WriterTypes
import ai.whylabs.services.whylogs.persistent.queue.PopSize
import java.time.temporal.ChronoUnit

class TestEnvVars : IEnvVars {
    override val writer = WriterTypes.WHYLABS
    override val whylabsApiEndpoint = "none"
    override val orgId = "org-1"
    override val ignoredKeys: Set<String> = setOf()
    override val emptyProfilesDatasetIds = emptyList<String>()
    override val disableAuth = false
    override val requestQueueingMode = WriteLayer.SQLITE
    override val requestQueueingEnabled = true
    override val profileStorageMode = WriteLayer.SQLITE
    override val requestQueueProcessingIncrement = PopSize.All
    override val whylabsApiKey = "key"
    override val whylogsPeriod = ChronoUnit.HOURS
    override val profileWritePeriod = ProfileWritePeriod.HOURS
    override val expectedApiKey = "password"
    override val s3Prefix = ""
    override val s3Bucket = "test-bucket"
    override val port = 8080
    override val debug = false
    override val kafkaConfig = null
    override val fileSystemWriterRoot = "whylogs-profiles"
}
