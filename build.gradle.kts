import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import org.gradle.api.credentials.HttpHeaderCredentials
import org.gradle.authentication.http.HttpHeaderAuthentication

plugins {
    idea
    kotlin("jvm") version "1.4.10"
    application
}
group = "ai.whylabs.services"
version = "1.0-SNAPSHOT"

// You'll need to generate an API token in Gitlab that has api permissions.
val gitlabMavenToken = System.getenv("MAVEN_TOKEN")

repositories {
    mavenCentral()
    maven {
        // https://gitlab.com/whylabs/core/songbird-java-client/-/packages
        url = uri("https://gitlab.com/api/v4/projects/22420498/packages/maven")
        name = "Gitlab"


        val headerName = if (System.getenv("CI_JOB_STAGE").isNullOrEmpty()) "Private-Token" else "Job-Token"
        credentials(HttpHeaderCredentials::class) {
            name = headerName
            value = gitlabMavenToken
        }

        authentication {
            create<HttpHeaderAuthentication>("header")
        }
    }
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }
}

tasks.withType<KotlinCompile>().configureEach {
    kotlinOptions {
        // config JVM target to 1.8 for kotlin compilation tasks
        jvmTarget = "1.8"
        // Silence warnings about using Kotlin's actor and similar types. They don't yet have
        // replacements that Kotlin recommends using. When that changes we can update them.
        freeCompilerArgs += listOf(
            "-Xuse-experimental=kotlinx.coroutines.ObsoleteCoroutinesApi"
        )
    }
}

dependencies {
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8:1.4.10")
    implementation("org.jetbrains.kotlin:kotlin-reflect:1.4.10")
    implementation("io.javalin:javalin:3.11.2")
    implementation("io.javalin:javalin-openapi:3.11.2")
    implementation("io.swagger.core.v3:swagger-core:2.1.5")
    implementation("org.webjars:swagger-ui:3.24.3")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.10.1")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.10.1")
    implementation("ai.whylabs:whylogs-core:0.1.0")
    implementation("org.apache.commons:commons-lang3:3.11")
    implementation("com.amazonaws:aws-java-sdk-s3:1.11.+")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.4.2")
    implementation("ai.whylabs:songbird-client:0.1-SNAPSHOT")
    runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.13.3")
    implementation("org.xerial:sqlite-jdbc:3.34.0")

    testImplementation("org.assertj:assertj-core:3.12.2")
    testImplementation(platform("org.junit:junit-bom:5.7.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

application {
    mainClassName = "ai.whylabs.services.whylogs.MainKt"
}

