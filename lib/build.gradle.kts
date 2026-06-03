plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlinx.rpc)
    `java-library`
}

repositories {
    mavenCentral()
    maven("https://redirector.kotlinlang.org/maven/kxrpc-grpc")
}

dependencies {
    implementation(libs.kotlinx.rpc.grpc.core)
    implementation(libs.kotlinx.rpc.protobuf.core)

    testImplementation("org.jetbrains.kotlin:kotlin-test")
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
