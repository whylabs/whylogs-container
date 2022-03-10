package ai.whylabs.services.whylogs.util

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.lang.invoke.MethodHandles

object LoggingUtil {

    /**
     * Util function for getting a loggert for a Kotlin file that has no top level class.
     * It will end up using the class name of the generated Java class, which looks something
     * like `FileNameKt`.
     */
    fun getLoggerForFile(): Logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass())
}
