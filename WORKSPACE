workspace(
    name = "com_google_api_gax_kotlin",
)

PROTOBUF_VERSION = "3.20.1"
GAX_VERSION = "2.18.1"
GRPC_JAVA_VERSION = "1.46.0"
GRPC_KT_VERSION = "1.3.0"
KOTLIN_SDK_VERSION = "1.6.21"

load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "com_google_protobuf",
    sha256 = "8b28fdd45bab62d15db232ec404248901842e5340299a57765e48abe8a80d930",
    strip_prefix = "protobuf-%s" % PROTOBUF_VERSION,
    urls = ["https://github.com/protocolbuffers/protobuf/archive/v%s.tar.gz" % PROTOBUF_VERSION],
)

http_archive(
    name = "bazel_skylib",
    sha256 = "af87959afe497dc8dfd4c6cb66e1279cb98ccc84284619ebfec27d9c09a903de",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/bazel-skylib/releases/download/1.2.0/bazel-skylib-1.2.0.tar.gz",
        "https://github.com/bazelbuild/bazel-skylib/releases/download/1.2.0/bazel-skylib-1.2.0.tar.gz",
    ],
)

http_archive(
    name = "rules_pkg",
    sha256 = "8a298e832762eda1830597d64fe7db58178aa84cd5926d76d5b744d6558941c2",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_pkg/releases/download/0.7.0/rules_pkg-0.7.0.tar.gz",
        "https://github.com/bazelbuild/rules_pkg/releases/download/0.7.0/rules_pkg-0.7.0.tar.gz",
    ],
)

rules_kotlin_version = "1.6.0-RC-3"
rules_kotlin_sha = "a57591404423a52bd6b18ebba7979e8cd2243534736c5c94d35c89718ea38f94"

http_archive(
    name = "io_bazel_rules_kotlin",
    urls = ["https://github.com/bazelbuild/rules_kotlin/releases/download/v%s/rules_kotlin_release.tgz" % rules_kotlin_version],
    sha256 = rules_kotlin_sha,
)

http_archive(
    name = "io_grpc_grpc_java",
    sha256 = "2f2ca0701cf23234e512f415318bfeae00036a980f6a83574264f41c0201e5cd",
    strip_prefix = "grpc-java-%s" % GRPC_JAVA_VERSION,
    url = "https://github.com/grpc/grpc-java/archive/refs/tags/v%s.zip" % GRPC_JAVA_VERSION,
)

http_archive(
    name = "io_grpc_grpc_proto",
    sha256 = "fe1ae9b0fba20adbb7304fc33232797354fefc13a30440e62033053bd7cdfecc",
    strip_prefix = "grpc-proto-856582e8a94d70b79de680133da90d301736baa1",
    urls = ["https://github.com/sgammon/grpc-proto/archive/856582e8a94d70b79de680133da90d301736baa1.tar.gz"],
)

http_archive(
    name = "com_github_grpc_grpc_kotlin",
    sha256 = "466d33303aac7e825822b402efa3dcfddd68e6f566ed79443634180bb75eab6e",
    strip_prefix = "grpc-kotlin-%s" % GRPC_KT_VERSION,
    url = "https://github.com/grpc/grpc-kotlin/archive/v%s.tar.gz" % GRPC_KT_VERSION,
)

#### ---- ####

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps", "PROTOBUF_MAVEN_ARTIFACTS")

protobuf_deps()

load("@bazel_skylib//:workspace.bzl", "bazel_skylib_workspace")

bazel_skylib_workspace()

load("@rules_pkg//:deps.bzl", "rules_pkg_dependencies")

rules_pkg_dependencies()

load("@io_bazel_rules_kotlin//kotlin:repositories.bzl", "kotlin_repositories", "kotlinc_version")

kotlin_repositories(
    compiler_release = kotlinc_version(
        release = KOTLIN_SDK_VERSION,  # just the numeric version
        sha256 = "632166fed89f3f430482f5aa07f2e20b923b72ef688c8f5a7df3aa1502c6d8ba",
    ),
)

load("@io_bazel_rules_kotlin//kotlin:core.bzl", "kt_register_toolchains")

kt_register_toolchains()

RULES_JVM_EXTERNAL_TAG = "4.2"

RULES_JVM_EXTERNAL_SHA = "cd1a77b7b02e8e008439ca76fd34f5b07aecb8c752961f9640dea15e9e5ba1ca"

http_archive(
    name = "rules_jvm_external",
    sha256 = RULES_JVM_EXTERNAL_SHA,
    strip_prefix = "rules_jvm_external-%s" % RULES_JVM_EXTERNAL_TAG,
    url = "https://github.com/bazelbuild/rules_jvm_external/archive/%s.zip" % RULES_JVM_EXTERNAL_TAG,
)

load("@rules_jvm_external//:repositories.bzl", "rules_jvm_external_deps")

rules_jvm_external_deps()

load("@rules_jvm_external//:setup.bzl", "rules_jvm_external_setup")

rules_jvm_external_setup()

load("@rules_jvm_external//:defs.bzl", "maven_install")
load("@com_github_grpc_grpc_kotlin//:repositories.bzl", "IO_GRPC_GRPC_KOTLIN_ARTIFACTS", "IO_GRPC_GRPC_KOTLIN_OVERRIDE_TARGETS")
load("@io_grpc_grpc_java//:repositories.bzl", "IO_GRPC_GRPC_JAVA_ARTIFACTS", "IO_GRPC_GRPC_JAVA_OVERRIDE_TARGETS")

INJECTED_JVM_ARTIFACTS = (
    [i for i in (
        IO_GRPC_GRPC_JAVA_ARTIFACTS +
        IO_GRPC_GRPC_KOTLIN_ARTIFACTS +
        PROTOBUF_MAVEN_ARTIFACTS
    ) if not (
        "guava" in i or
        "kotlinx-coroutines" in i or
        "error_prone_annotations" in i or
        "truth" in i
    )]
)

maven_install(
    artifacts = [
        "com.google.api:gax:%s" % GAX_VERSION,
        "com.google.api:gax-grpc:%s" % GAX_VERSION,
        "com.google.guava:failureaccess:1.0.1",
        "com.google.guava:guava:31.1-android",
        "com.google.errorprone:error_prone_annotations:2.14.0",
        "com.google.protobuf:protobuf-java:%s" % PROTOBUF_VERSION,
        "com.google.protobuf:protobuf-java-util:%s" % PROTOBUF_VERSION,
        "com.google.protobuf:protobuf-kotlin:%s" % PROTOBUF_VERSION,
        "com.google.truth:truth:1.1.3",
        "com.google.truth.extensions:truth-proto-extension:1.1.3",
        "com.google.truth.extensions:truth-java8-extension:1.1.3",
        "io.grpc:grpc-all:%s" % GRPC_JAVA_VERSION,
        "io.grpc:grpc-alts:%s" % GRPC_JAVA_VERSION,
        "io.grpc:grpc-android:%s" % GRPC_JAVA_VERSION,
        "io.grpc:grpc-auth:%s" % GRPC_JAVA_VERSION,
        "io.grpc:grpc-core:%s" % GRPC_JAVA_VERSION,
        "io.grpc:grpc-context:%s" % GRPC_JAVA_VERSION,
        "io.grpc:grpc-grpclb:%s" % GRPC_JAVA_VERSION,
        "io.grpc:grpc-stub:%s" % GRPC_JAVA_VERSION,
        "io.grpc:grpc-testing:%s" % GRPC_JAVA_VERSION,
        "io.grpc:grpc-netty:%s" % GRPC_JAVA_VERSION,
        "io.grpc:grpc-netty-shaded:%s" % GRPC_JAVA_VERSION,
        "io.grpc:grpc-okhttp:%s" % GRPC_JAVA_VERSION,
        "io.grpc:grpc-protobuf:%s" % GRPC_JAVA_VERSION,
        "io.grpc:grpc-protobuf-lite:%s" % GRPC_JAVA_VERSION,
        "io.grpc:grpc-services:%s" % GRPC_JAVA_VERSION,
        "io.grpc:grpc-xds:%s" % GRPC_JAVA_VERSION,
        "io.grpc:grpc-kotlin-stub:1.3.0",
        "javax.annotation:javax.annotation-api:1.3.2",
        "org.jetbrains.kotlinx:kotlinx-coroutines-core:1.6.2",
        "org.jetbrains.kotlinx:kotlinx-coroutines-core-jvm:1.6.2",
        "org.jetbrains.kotlinx:kotlinx-coroutines-jdk8:1.6.2",
        "org.jetbrains.kotlinx:kotlinx-coroutines-jdk9:1.6.2",
        "org.jetbrains.kotlinx:kotlinx-coroutines-guava:1.6.2",
        "org.jetbrains.kotlin:kotlin-stdlib:%s" % KOTLIN_SDK_VERSION,
        "org.jetbrains.kotlin:kotlin-stdlib-jdk7:%s" % KOTLIN_SDK_VERSION,
        "org.jetbrains.kotlin:kotlin-stdlib-jdk8:%s" % KOTLIN_SDK_VERSION,
    ] + INJECTED_JVM_ARTIFACTS,
    fetch_javadoc = True,
    fetch_sources = True,
    generate_compat_repositories = True,
    maven_install_json = "@//:maven_install.json",
    override_targets = dict(
        IO_GRPC_GRPC_KOTLIN_OVERRIDE_TARGETS.items() +
        IO_GRPC_GRPC_JAVA_OVERRIDE_TARGETS.items(),
    ),
    repositories = [
        "https://repo1.maven.org/maven2",
        "https://maven.google.com",
        "https://jcenter.bintray.com/",
        "https://repo.maven.apache.org/maven2",
    ],
    strict_visibility = True,
    version_conflict_policy = "pinned",
)

load("@maven//:defs.bzl", "pinned_maven_install")

pinned_maven_install()

load("@maven//:compat.bzl", "compat_repositories")

compat_repositories()
