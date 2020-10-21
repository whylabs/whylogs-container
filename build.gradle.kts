import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    idea
    kotlin("jvm") version "1.4.10"
    application
}
group = "ai.whylabs.services"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
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
    runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.13.3")
    testImplementation(kotlin("test-testng"))
}
tasks.withType<KotlinCompile>() {
    kotlinOptions.jvmTarget = "1.8"
}
application {
    mainClassName = "ai.whylabs.services.whylogs.MainKt"
}