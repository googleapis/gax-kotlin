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

import com.google.protobuf.gradle.*

plugins {
    idea
    maven
    kotlin("jvm")
    `java-library`
    id("com.google.protobuf") version "0.8.7"
    jacoco
}

jacoco {
    toolVersion = "0.8.2"
}

dependencies {
    implementation(kotlin("stdlib"))

    implementation(project(":kgax-core"))

    implementation("javax.annotation:javax.annotation-api:${ext["javax_annotation_version"]}")

    api("io.grpc:grpc-stub:${ext["grpc_version"]}")
    api("io.grpc:grpc-auth:${ext["grpc_version"]}")
    implementation("io.grpc:grpc-protobuf-lite:${ext["grpc_version"]}")
    api("com.google.auth:google-auth-library-oauth2-http:0.9.1")
    api("com.google.auth:google-auth-library-credentials:0.9.1")

    testImplementation(kotlin("test"))
    testImplementation(kotlin("test-junit"))
    testImplementation("junit:junit:${ext["junit_version"]}")
    testImplementation("com.nhaarman:mockito-kotlin:${ext["mockito_kotlin_version"]}")
    testImplementation("com.google.truth:truth:${ext["truth_version"]}")
    testImplementation("io.grpc:grpc-netty-shaded:${ext["grpc_version"]}")
}

base {
    archivesBaseName = "kgax-grpc-base"
}


java {
    sourceSets {
        getByName("test") {
            withGroovyBuilder {
                "proto" {
                    "srcDir"("$projectDir/../kgax-common-protos")
                }
            }
        }
    }
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:${ext["protoc_version"]}"
    }
    plugins {
        id("javalite") {
            artifact = "com.google.protobuf:protoc-gen-javalite:${ext["protoc_gen_java_lite_version"]}"
        }
    }
    generateProtoTasks {
        ofSourceSet("main").forEach {
            it.builtins {
                remove("java")
            }
        }
        ofSourceSet("test").forEach {
            it.builtins {
                remove("java")
            }
            it.plugins {
                id("javalite")
            }
        }
    }
}
