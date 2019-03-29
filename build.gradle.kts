/*
 * Copyright 2018 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.gradle.api.tasks.testing.logging.TestLogEvent
import org.jetbrains.dokka.gradle.DokkaTask
import org.jetbrains.dokka.gradle.PackageOptions

plugins {
    idea
    maven
    kotlin("jvm") version "1.3.21"
    id("org.jetbrains.dokka") version "0.9.17"
    id("jacoco")
}

dependencies {
    compile(kotlin("stdlib"))

    compile(project(":kgax-core"))
    compile(project(":kgax-grpc"))
    compile(project(":kgax-grpc-android"))

    compile(project(":examples"))

    compile(project(":showcase-test"))
}

base {
    archivesBaseName = "kgax"
}

allprojects {
    group = "com.google.api"
    version = "0.3.0-SNAPSHOT"

    ext {
        set("javax_annotation_version", "1.3.2")
        set("protobuf_version", "3.6.1")
        set("protoc_version", "3.6.1")
        set("protobuf_lite_version", "3.0.1")
        set("protoc_gen_javalite_version", "3.0.0")
        set("grpc_version", "1.16.1")

        set("junit_version", "4.12")
        set("mockito_kotlin_version", "1.6.0")
        set("truth_version", "0.41")
    }

    buildscript {
        repositories {
            google()
            mavenCentral()
            jcenter()
        }
    }

    repositories {
        google()
        mavenCentral()
        jcenter()
    }
}

subprojects {
    val ktlintImplementation by configurations.creating

    dependencies {
        ktlintImplementation("com.github.shyiko:ktlint:0.30.0")
    }

    afterEvaluate {
        tasks {
            val test = getByName("test")
            val check = getByName("check")

            withType<Test> {
                testLogging {
                    events = setOf(TestLogEvent.PASSED, TestLogEvent.SKIPPED, TestLogEvent.FAILED)
                }
            }

            withType<JacocoReport> {
                reports {
                    xml.isEnabled = true
                    html.isEnabled = true
                }
                sourceDirectories = files(listOf("src/main/kotlin"))
                test.finalizedBy(this)
            }

            val ktlint by creating(JavaExec::class) {
                group = "verification"
                description = "Check Kotlin code style."
                main = "com.github.shyiko.ktlint.Main"
                classpath = ktlintImplementation
                args = listOf("src/**/*.kt", "test/**/*.kt")
            }
            check.dependsOn(ktlint)

            val ktlintFormat by creating(JavaExec::class) {
                group = "formatting"
                description = "Fix Kotlin code style deviations."
                main = "com.github.shyiko.ktlint.Main"
                classpath = ktlintImplementation
                args = listOf("-F", "src/**/*.kt", "test/**/*.kt")
            }
        }
    }
}

tasks {
    val dokka by getting(DokkaTask::class) {
        outputFormat = "html"
        outputDirectory = "$buildDir/docs"
        sourceDirs = files(subprojects.map { p -> File(p.projectDir, "/src/main/kotlin") })

        packageOptions(delegateClosureOf<PackageOptions> {
            prefix = "com.google.protobuf"
            suppress = true
        })
    }
}
