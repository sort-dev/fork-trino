plugins {
    java
}

group = "io.trino"
version = "480"

val trinoVersion = "480"
val duckdbVersion = "1.5.2.0"

java {
    sourceCompatibility = JavaVersion.VERSION_25
    targetCompatibility = JavaVersion.VERSION_25
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

repositories {
    mavenCentral()
}

// Use Trino BOM for all managed dependency versions
dependencies {
    // BOM import — controls versions for io.trino, io.airlift, jackson, opentelemetry, etc.
    implementation(platform("io.trino:trino-root:$trinoVersion"))
    testImplementation(platform("io.trino:trino-root:$trinoVersion"))

    // Core implementation dependencies (versions from BOM)
    implementation("com.google.guava:guava")
    implementation("com.google.inject:guice") {
        artifact {
            classifier = "classes"
        }
    }
    implementation("com.zaxxer:HikariCP:5.0.1") // not in Trino BOM
    implementation("io.airlift:bootstrap")
    implementation("io.airlift:configuration")
    implementation("io.airlift:json")
    implementation("io.airlift:log")
    implementation("io.airlift:units")
    implementation("io.trino:trino-filesystem")
    implementation("io.trino:trino-filesystem-manager")
    implementation("io.trino:trino-hive")
    implementation("io.trino:trino-memory-context")
    implementation("io.trino:trino-parquet")
    implementation("io.trino:trino-plugin-toolkit")
    implementation("jakarta.validation:jakarta.validation-api")
    implementation("joda-time:joda-time")
    implementation("org.apache.parquet:parquet-column")
    implementation("org.apache.parquet:parquet-format-structures") {
        exclude(group = "javax.annotation", module = "javax.annotation-api")
    }
    implementation("org.weakref:jmxutils")

    // SPI dependencies — provided by Trino at runtime (compileOnly = Maven provided)
    compileOnly("com.fasterxml.jackson.core:jackson-annotations")
    compileOnly("io.airlift:slice")
    compileOnly("io.opentelemetry:opentelemetry-api")
    compileOnly("io.opentelemetry:opentelemetry-api-incubator")
    compileOnly("io.opentelemetry:opentelemetry-context")
    compileOnly("io.trino:trino-spi")

    // Runtime-only dependencies
    runtimeOnly("io.airlift:log-manager")
    runtimeOnly("org.postgresql:postgresql")

    // Test dependencies
    testImplementation("io.airlift:testing")
    testImplementation("io.trino:trino-hdfs")
    testImplementation("io.trino:trino-main")
    testImplementation("io.trino:trino-testing")
    testImplementation("org.assertj:assertj-core")
    testImplementation("org.duckdb:duckdb_jdbc:$duckdbVersion") // not in Trino BOM
    testImplementation("org.junit.jupiter:junit-jupiter-api")
    testImplementation("org.testcontainers:testcontainers")
    testImplementation("org.testcontainers:testcontainers-postgresql")

    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    // compileOnly deps also needed at test time
    testCompileOnly("com.fasterxml.jackson.core:jackson-annotations")
    testImplementation("io.airlift:slice")
    testImplementation("io.trino:trino-spi")
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
    options.compilerArgs.addAll(listOf(
        "--enable-preview",
    ))
}

tasks.test {
    useJUnitPlatform()

    maxHeapSize = "3g"
    minHeapSize = "3g"

    systemProperty("junit.jupiter.execution.parallel.enabled", "true")
    systemProperty("junit.jupiter.execution.parallel.mode.default", "concurrent")
    systemProperty("junit.jupiter.execution.parallel.mode.classes.default", "concurrent")
    systemProperty("junit.jupiter.extensions.autodetection.enabled", "true")
    // Podman Docker API compatibility rejects status=restarting filter in ReportLeakedContainers
    systemProperty("ReportLeakedContainers.disabled", "true")

    jvmArgs(
        "-XX:+ExitOnOutOfMemoryError",
        "-XX:+HeapDumpOnOutOfMemoryError",
        "-XX:-OmitStackTraceInFastThrow",
        "-XX:G1HeapRegionSize=32M",
        "-XX:+UnlockDiagnosticVMOptions",
        "--add-modules=jdk.incubator.vector",
        "--sun-misc-unsafe-memory-access=allow",
        "--enable-native-access=ALL-UNNAMED",
    )
}

// Plugin assembly — collect all runtime JARs into a flat directory (Trino plugin layout)
val pluginAssemble by tasks.registering(Copy::class) {
    dependsOn(tasks.jar)
    into(layout.buildDirectory.dir("trino-plugin/trino-ducklake-$version"))
    from(tasks.jar)
    from(configurations.runtimeClasspath)
}

tasks.build {
    dependsOn(pluginAssemble)
}
