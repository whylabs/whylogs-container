package ai.whylabs.services.whylogs.core

import io.mockk.coEvery
import io.mockk.mockk
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.util.Date

internal class DebugInfoManagerTest {

    @Test
    fun `errors are capped`() = runBlocking {
        val maxErrors = 5
        val profileStore: ProfileStore = mockk(relaxed = true)
        coEvery { profileStore.profiles.map.size() } returns 0
        val debugInfoManager = DebugInfoManager(profileStore = profileStore, envVars = TestEnvVars(), maxErrors = maxErrors)

        val time = Date()
        repeat(maxErrors * 10) {
            debugInfoManager.send(DebugInfoMessage.ProfileWriteFailuresMessage(IllegalStateException("wrong", IllegalStateException("cause")), time))
        }

        // Just to see the state in unit test logs
        debugInfoManager.send(DebugInfoMessage.LogMessage)

        val done = CompletableDeferred<DebugInfo>()
        debugInfoManager.send(DebugInfoMessage.GetStateMessage(done))

        val debugInfo = done.await()
        Assertions.assertEquals(maxErrors, debugInfo.profilesWriteFailureCauses.size)
        Assertions.assertEquals((maxErrors * 10).toLong(), debugInfo.profilesWriteFailures)
    }
}
