package ai.whylabs.services.whylogs.core

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.whylogs.core.DatasetProfile
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.spyk
import java.time.Instant
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

internal class RequestProfileUtilKtTest {

    private val mapper = jacksonObjectMapper()

    @Test
    fun `single level of nesting tracks`() {
        val data = mapper.readValue<Map<String, Any>>(
            """
            {
                "a": 2,
                "b": 3,
                "c": {
                    "foo": "bar"
                }
            }
        """.trimIndent()
        )

        val profile = spyk(DatasetProfile("", Instant.ofEpochMilli(0)))
        val actual = mutableMapOf<String, String>()

        val capturedFeature = slot<String>()
        val capturedValue = slot<Any>()
        every {
            profile.track(capture(capturedFeature), capture(capturedValue))
        } answers {
            actual[capturedFeature.captured] = capturedValue.captured.toString()
        }

        profile.mergeNested(data)

        val expected = mapOf<String, Any>("a" to "2", "b" to "3", "c.foo" to "bar")
        Assertions.assertEquals(expected, actual)
    }

    @Test
    fun `multiple levels of nesting tracks`() {
        val data = mapper.readValue<Map<String, Any>>(
            """
            {
                "a": 2,
                "b": 3,
                "c": {
                    "d": {
                        "foo": "bar"
                    },
                    "e": {
                        "f": {
                            "why": "labs"
                        }
                    }
                },
                "g": 5
            }
        """.trimIndent()
        )

        val profile = spyk(DatasetProfile("", Instant.ofEpochMilli(0)))
        val actual = mutableMapOf<String, String>()

        val capturedFeature = slot<String>()
        val capturedValue = slot<Any>()
        every {
            profile.track(capture(capturedFeature), capture(capturedValue))
        } answers {
            actual[capturedFeature.captured] = capturedValue.captured.toString()
        }

        profile.mergeNested(data)

        val expected = mapOf<String, Any>(
            "a" to "2",
            "b" to "3",
            "c.d.foo" to "bar",
            "c.e.f.why" to "labs",
            "g" to "5",
        )
        Assertions.assertEquals(expected, actual)
    }
}