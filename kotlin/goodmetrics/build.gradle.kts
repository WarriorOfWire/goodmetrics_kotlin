import com.google.protobuf.gradle.builtins
import com.google.protobuf.gradle.generateProtoTasks
import com.google.protobuf.gradle.id
import com.google.protobuf.gradle.plugins
import com.google.protobuf.gradle.protobuf
import com.google.protobuf.gradle.protoc

plugins {
    id("org.jetbrains.kotlin.jvm")

    id("com.google.protobuf") version "0.8.19"

    // Apply the java-library plugin for API and implementation separation.
    `java-library`
    `maven-publish`
    signing

    idea
}

group = rootProject.group
version = rootProject.version

val signingKey = System.getenv("SIGNING_KEY")
val signingPassword = System.getenv("SIGNING_PASSWORD")

java {
    withJavadocJar()
    withSourcesJar()
}

repositories {
    // Use Maven Central for resolving dependencies.
    mavenCentral()
}

val protoVersion = "3.21.2"
val grpcVersion = "1.47.0"
val grpcKtVersion = "1.3.0"
val coroutinesVersion = "1.6.3"

dependencies {
    // Align versions of all Kotlin components
    implementation(platform("org.jetbrains.kotlin:kotlin-bom"))
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:$coroutinesVersion")
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")

    implementation("io.grpc:grpc-stub:$grpcVersion")
    implementation("io.grpc:grpc-netty:$grpcVersion")
    implementation("io.grpc:grpc-kotlin-stub:$grpcKtVersion")
    implementation("io.grpc:grpc-protobuf:$grpcVersion")
    implementation("com.google.protobuf:protobuf-kotlin:$protoVersion")

    if (JavaVersion.current().isJava9Compatible) {
        // Workaround for @javax.annotation.Generated
        // see: https://github.com/grpc/grpc-java/issues/3633
        compileOnly("javax.annotation:javax.annotation-api:1.3.1")
    }

    testImplementation("org.jetbrains.kotlin:kotlin-test")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit")

    protobuf(files("src/proto"))
}

kotlin {
    jvmToolchain {
        languageVersion.set(JavaLanguageVersion.of("17"))
    }
}

tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile>().all {
    kotlinOptions {
        freeCompilerArgs = listOf("-Xopt-in=kotlin.RequiresOptIn", "-Xopt-in=kotlin.time.ExperimentalTime")
    }
}

protobuf {
    // Configure the protoc executable
    protoc {
        // Download from repositories
        artifact = "com.google.protobuf:protoc:$protoVersion"
    }
    plugins {
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:$grpcVersion"
        }
        id("grpckt") {
            artifact = "io.grpc:protoc-gen-grpc-kotlin:$grpcKtVersion:jdk8@jar"
        }
    }
    generateProtoTasks {
        all().forEach {
            it.plugins {
                id("grpc")
                id("grpckt")
            }
            it.builtins {
                id("kotlin")
            }
        }
    }
}

tasks.kotlinSourcesJar {
    archiveClassifier.set("sources")
}
tasks.javadoc {
    if (JavaVersion.current().isJava9Compatible) {
        (options as StandardJavadocDocletOptions).addBooleanOption("html5", true)
    }
}

publishing {
    publications {
        create<MavenPublication>("goodmetrics") {
            artifact(tasks.named("javadocJar"))
            artifact(tasks["sourcesJar"])

            artifactId = "goodmetrics-kotlin"

            println("components:")
            for (c in components) {
                println("  ${c.name}")
            }
            from(components["kotlin"])

            versionMapping {
                usage("java-api") {
                    fromResolutionOf("runtimeClasspath")
                }
                usage("java-runtime") {
                    fromResolutionResult()
                }
            }

            pom {
                name.set("goodmetrics")
                description.set("A metrics recording library that is good")
                url.set("https://github.com/kvc0/goodmetrics_kotlin/blob/main/README.md")
                properties.set(mapOf())
                licenses {
                    license {
                        name.set("The Apache License, Version 2.0")
                        url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                }
                developers {
                    developer {
                        id.set("kvc0")
                        name.set("Kenny")
                        email.set("3454741+kvc0@users.noreply.github.com")
                    }
                }
                scm {
                    connection.set("scm:git:git://github.com:kvc0/goodmetrics_kotlin.git")
                    developerConnection.set("scm:git:ssh://git@github.com:kvc0/goodmetrics_kotlin.git")
                    url.set("https://github.com/kvc0/goodmetrics_kotlin")
                }
            }
        }
    }
}

signing {
    useInMemoryPgpKeys(signingKey, signingPassword)
    println("\npublications:")
    for (p in publishing.publications) {
        println("  ${p.name}")
    }

    sign(publishing.publications["goodmetrics"])
}
