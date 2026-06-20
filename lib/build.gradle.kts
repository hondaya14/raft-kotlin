plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlinx.rpc)
    `java-library`
    application
}

repositories {
    mavenCentral()
    maven("https://redirector.kotlinlang.org/maven/kxrpc-grpc")
}

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")
    implementation(libs.kotlinx.rpc.grpc.core)
    implementation(libs.kotlinx.rpc.protobuf.core)
    runtimeOnly(libs.grpc.netty.shaded)

    testImplementation("org.jetbrains.kotlin:kotlin-test")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.10.2")
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

rpc {
    protoc()
}

tasks.named<Test>("test") {
    useJUnitPlatform()
}

application {
    mainClass = "co.hondaya.raft.demo.DockerComposeNodeKt"
}
