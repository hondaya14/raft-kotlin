rootProject.name = "raft-kotlin"
include("lib")

pluginManagement {
    repositories {
        maven("https://redirector.kotlinlang.org/maven/kxrpc-grpc")
        gradlePluginPortal()
        mavenCentral()
    }
}

dependencyResolutionManagement {
    repositories {
        mavenCentral()
        maven("https://redirector.kotlinlang.org/maven/kxrpc-grpc")
    }
}
