plugins {
    // Apply the org.jetbrains.kotlin.jvm Plugin to add support for Kotlin.
    id("org.jetbrains.kotlin.jvm")

    id("me.champeau.jmh") version "0.6.6"

    `java-library`

    idea
}

repositories {
    mavenCentral()
}

dependencies {
    // Align versions of all Kotlin components
    jmhImplementation(platform("org.jetbrains.kotlin:kotlin-bom"))
    jmhImplementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")

    jmhImplementation("org.openjdk.jmh:jmh-core:1.34")
    jmhImplementation("org.openjdk.jmh:jmh-generator-annprocess:1.34")

    implementation(project(":kotlin:goodmetrics"))
}

kotlin {
    jvmToolchain {
        (this as JavaToolchainSpec).languageVersion.set(JavaLanguageVersion.of("17"))
    }
}

jmh {
    jmhVersion.set("1.34")
//    resultFormat.set("JSON")
//    resultsFile.set(File("jmhresults.json"))
}
