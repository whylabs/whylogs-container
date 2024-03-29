package ai.whylabs.services.whylogs.core

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.whylogs.core.DatasetProfile
import io.mockk.every
import io.mockk.slot
import io.mockk.spyk
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.time.Instant

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

        profile.mergeNested(data, setOf())

        val expected = mapOf<String, Any>("a" to "2", "b" to "3", "c.foo" to "bar")
        Assertions.assertEquals(expected, actual)
    }

    @Test
    fun `ignoring keys work`() {
        val ignored = setOf("a", "b")
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

        profile.mergeNested(data, ignored)

        val expected = mapOf<String, Any>("c.foo" to "bar")
        Assertions.assertEquals(expected, actual)
    }

    @Test
    fun `ignoring keys work in multi logs`() {
        val ignored = setOf("a", "b")
        val profile = spyk(DatasetProfile("", Instant.ofEpochMilli(0)))
        val actual = mutableMapOf<String, MutableList<Int>>()

        val capturedFeature = slot<String>()
        val capturedValue = slot<Int>()
        every {
            profile.track(capture(capturedFeature), capture(capturedValue))
        } answers {
            val value = capturedValue.captured
            actual.compute(capturedFeature.captured) { _, current -> current?.apply { add(value) } ?: mutableListOf(value) }
        }

        val request = LogRequest(
            datasetId = "1",
            tags = null,
            single = null,
            multiple = MultiLog(
                columns = listOf("a", "b", "c"),
                data = listOf(
                    listOf(1, 2, 3),
                    listOf(4, 5, 6),
                    listOf(7, 8, 9),
                )
            )
        )

        profile.merge(request, ignored)

        val expected = mapOf("c" to listOf(3, 6, 9))
        Assertions.assertEquals(expected, actual)
    }

    @Test
    fun `ignoring keys work in multi logs when empty`() {
        val ignored = setOf<String>()
        val profile = spyk(DatasetProfile("", Instant.ofEpochMilli(0)))
        val actual = mutableMapOf<String, MutableList<Int>>()

        val capturedFeature = slot<String>()
        val capturedValue = slot<Int>()
        every {
            profile.track(capture(capturedFeature), capture(capturedValue))
        } answers {
            val value = capturedValue.captured
            actual.compute(capturedFeature.captured) { _, current -> current?.apply { add(value) } ?: mutableListOf(value) }
        }

        val request = LogRequest(
            datasetId = "1",
            tags = null,
            single = null,
            multiple = MultiLog(
                columns = listOf("a", "b", "c"),
                data = listOf(
                    listOf(1, 2, 3),
                    listOf(4, 5, 6),
                    listOf(7, 8, 9),
                )
            )
        )

        profile.merge(request, ignored)

        val expected = mapOf(
            "a" to listOf(1, 4, 7),
            "b" to listOf(2, 5, 8),
            "c" to listOf(3, 6, 9)
        )
        Assertions.assertEquals(expected, actual)
    }

    @Test
    fun `ignoring nested keys work`() {
        val ignored = setOf("c.foo")
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

        profile.mergeNested(data, ignored)

        val expected = mapOf<String, Any>("a" to "2", "b" to "3")
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

        profile.mergeNested(data, setOf())

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
