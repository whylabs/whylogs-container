import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    idea
    kotlin("jvm") version "1.4.10"
    application
    id("org.jlleitschuh.gradle.ktlint") version "10.2.1"
    id("com.adarshr.test-logger") version "3.2.0"
}
group = "ai.whylabs.services"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

tasks {
    test {
        useJUnitPlatform()
        testLogging {
            events("passed", "skipped", "failed")
        }
    }
}

testlogger {
    theme = com.adarshr.gradle.testlogger.theme.ThemeType.MOCHA
    showFullStackTraces = true

    // Show output for all tests. Sometimes tests pass by mistake while logging errors and this makes
    // it possible to see that.
    showStandardStreams = true
    showPassedStandardStreams = true
    showSkippedStandardStreams = true
    showFailedStandardStreams = true
}

tasks.withType<KotlinCompile>().configureEach {
    kotlinOptions {
        // config JVM target to 1.8 for kotlin compilation tasks
        jvmTarget = "1.8"
        // Silence warnings about using Kotlin's actor and similar types. They don't yet have
        // replacements that Kotlin recommends using. When that changes we can update them.
        freeCompilerArgs = freeCompilerArgs + listOf(
            "-Xopt-in=kotlinx.coroutines.ObsoleteCoroutinesApi"
        )
    }
}

sourceSets {
    create("integTest") {
        compileClasspath += sourceSets.main.get().output + sourceSets.test.get().output
        runtimeClasspath += sourceSets.main.get().output + sourceSets.test.get().output

        java {
            srcDir("src/integ/kotlin")
        }
    }
}

val integTestImplementation: Configuration by configurations.getting {
    extendsFrom(configurations.testImplementation.get())
}
val integTestRuntimeOnly: Configuration by configurations.getting {
    extendsFrom(configurations.testRuntimeOnly.get())
}

val integrationTest = task<Test>("integTest") {
    testClassesDirs = sourceSets["integTest"].output.classesDirs
    classpath = sourceSets["integTest"].runtimeClasspath
    shouldRunAfter("test")
    useJUnitPlatform()
}

val javalinVersion = "4.4.0"

dependencies {
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8:1.4.10")
    implementation("org.jetbrains.kotlin:kotlin-reflect:1.4.10")
    implementation("io.javalin:javalin:$javalinVersion")
    implementation("io.javalin:javalin-openapi:$javalinVersion")
    implementation("io.swagger.core.v3:swagger-core:2.1.13")
    implementation("org.webjars:swagger-ui:3.24.3")

    implementation("org.apache.kafka:kafka-streams:3.1.0")
    implementation("org.apache.kafka:kafka-clients:3.1.0")

    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.12.3")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.10.1")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.10.1")

    implementation("io.sentry:sentry:5.7.2")

    // AWS
    implementation("com.amazonaws:aws-java-sdk-s3:1.11.+")
    implementation("com.amazonaws:aws-java-sdk-s3")

    implementation("org.apache.commons:commons-lang3:3.11")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.4.2")
    runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.16.0")
    implementation("org.xerial:sqlite-jdbc:3.34.0")
    implementation("com.michael-bull.kotlin-retry:kotlin-retry:1.0.8")

    // WhyLabs
    implementation("ai.whylabs:whylogs-java-core:0.1.2-b7")
    implementation("ai.whylabs:whylabs-api-client:0.1.8")

    // testing
    testImplementation("io.mockk:mockk:1.10.6")
    testImplementation("org.assertj:assertj-core:3.12.2")
    testImplementation(platform("org.junit:junit-bom:5.7.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

application {
    mainClass.set("ai.whylabs.services.whylogs.MainKt")
    // This lets us see which coroutine work is happening on. Important for debugging.
    applicationDefaultJvmArgs = listOf("-Dkotlinx.coroutines.debug")
}
