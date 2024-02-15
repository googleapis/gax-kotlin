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

plugins {
    idea
    maven
    kotlin("jvm")
    `java-library`
}

dependencies {
    compile(kotlin("stdlib"))

    implementation("javax.annotation:javax.annotation-api:${ext["javax_annotation_version"]}")

    api("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.1.0")
    api("org.jetbrains.kotlinx:kotlinx-coroutines-guava:1.8.0")
    
    testImplementation(kotlin("test"))
    testImplementation(kotlin("test-junit"))
    testImplementation("junit:junit:${ext["junit_version"]}")
    testImplementation("com.nhaarman:mockito-kotlin:${ext["mockito_kotlin_version"]}")
    testImplementation("com.google.truth:truth:${ext["truth_version"]}")
}

base {
    archivesBaseName = "kgax-core"
}
