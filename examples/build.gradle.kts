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

import com.google.protobuf.gradle.protobuf
import com.google.protobuf.gradle.protoc
import com.google.protobuf.gradle.plugins
import com.google.protobuf.gradle.id
import com.google.protobuf.gradle.generateProtoTasks
import com.google.protobuf.gradle.ofSourceSet

plugins {
    idea
    maven
    kotlin("jvm")
    application
    id("com.google.protobuf") version "0.8.7"
}

base {
    archivesBaseName = "kgax-examples"
}

application {
    mainClassName = "example.Main"
}

defaultTasks = listOf("run")

dependencies {
    implementation(kotlin("stdlib"))

    implementation("javax.annotation:javax.annotation-api:${ext["javax_annotation_version"]}")

    implementation(project(":kgax-grpc"))

    testImplementation(kotlin("test"))
    testImplementation(kotlin("test-junit"))
    testImplementation("junit:junit:${ext["junit_version"]}")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:${ext["protoc_version"]}"
    }
    plugins {
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:${ext["grpc_version"]}"
        }
    }
    generateProtoTasks {
        ofSourceSet("main").forEach {
            it.plugins {
                id("grpc")
            }
        }
    }
}
