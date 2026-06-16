# Repository Guidelines

## Project Structure & Module Organization

This repository is a Kotlin/JVM implementation of the Raft consensus algorithm. The Gradle root project is `raft-kotlin` and currently includes one module, `lib`.

- `lib/src/main/kotlin/co/hondaya/` contains Kotlin library code.
- `lib/src/main/proto/raft/v1/` contains protobuf and gRPC service definitions for Raft RPCs.
- `lib/src/test/kotlin/` is the expected location for unit tests when adding behavior.
- `docs/` holds design notes; update it when protocol or architecture decisions change.
- `build/` and `lib/build/` are generated Gradle outputs and should not be edited or committed.

## Build, Test, and Development Commands

Use the Gradle wrapper so builds use the project-pinned Gradle version.

- `./gradlew build` compiles all modules, generates protobuf/RPC sources, runs tests, and builds artifacts.
- `./gradlew test` runs the JVM test suite with JUnit Platform.
- `./gradlew :lib:jar` builds the library JAR at `lib/build/libs/lib.jar`.
- `./gradlew clean` removes generated build outputs.

The build uses Java toolchain 21, Kotlin JVM, and the `org.jetbrains.kotlinx.rpc.plugin` plugin. Dependency and plugin versions live in `gradle/libs.versions.toml`.

## Coding Style & Naming Conventions

Write Kotlin using standard Kotlin conventions: 4-space indentation, `PascalCase` for classes/enums, `camelCase` for functions and properties, and uppercase enum constants as used by `NodeState`. Keep package names under `co.hondaya` unless adding generated protocol code, which uses `co.hondaya.raft.protocol.v1`.

For protobuf files, use `snake_case` fields, stable numeric tags, and package paths that match the versioned API directory, for example `raft/v1/raft.proto`. Do not reuse removed protobuf tag numbers.

## Testing Guidelines

Tests use `kotlin-test` and run through JUnit Platform. Place tests beside the module they cover under `lib/src/test/kotlin/`, with names like `NodeStateTest` or `RaftLogTest`. Prefer focused unit tests for state transitions, election behavior, log matching, and protocol serialization. Run `./gradlew test` before opening a pull request.

## Commit & Pull Request Guidelines

The existing history uses short, imperative commit messages such as `Create proto for node's rpc and message` and `modified design.md`. Keep commits focused and describe the behavior or artifact changed.

Pull requests should include a concise summary, relevant design or protocol notes, linked issues when available, and the Gradle commands run for verification. Include screenshots only for documentation or visual changes.

## Agent-Specific Instructions

Respect generated code boundaries: edit source files and protobuf definitions, then let Gradle regenerate outputs. Avoid broad refactors while implementing Raft behavior; keep changes tied to a single protocol, state, or storage concern.
