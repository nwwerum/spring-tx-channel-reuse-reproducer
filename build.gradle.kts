plugins {
    java
}

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.springframework:spring-framework-bom:7.0.10"))
    testImplementation(platform("org.junit:junit-bom:6.1.3"))

    testImplementation("org.springframework.amqp:spring-rabbit:4.1.2")
    testImplementation("org.springframework:spring-jdbc")
    testImplementation("com.h2database:h2:2.5.252")

    testImplementation("org.testcontainers:rabbitmq:1.21.4")
    testImplementation("org.testcontainers:junit-jupiter:1.21.4")
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testRuntimeOnly("org.slf4j:slf4j-simple:2.0.17")
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        showStandardStreams = true
        events("passed", "failed")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.SHORT
    }
}
