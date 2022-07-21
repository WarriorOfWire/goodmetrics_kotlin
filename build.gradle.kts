plugins {
    id("io.github.gradle-nexus.publish-plugin") version "1.1.0"
}

group = "com.kvc0"
version = System.getenv("VERSION") ?: "${gitVersion()}-SNAPSHOT"

val ossrhUsername = System.getenv("OSSRH_USERNAME")
val ossrhPassword = System.getenv("OSSRH_PASSWORD")

fun gitVersion(): String {
    val stdout = java.io.ByteArrayOutputStream()
    exec {
        commandLine = listOf("git", "describe", "--tags")
        standardOutput = stdout
    }
    return stdout.toString().trim()
}

nexusPublishing {
    repositories {
        sonatype {
            nexusUrl.set(uri("https://s01.oss.sonatype.org/service/local/"))
            snapshotRepositoryUrl.set(uri("https://s01.oss.sonatype.org/content/repositories/snapshots/"))
            username.set(ossrhUsername)
            password.set(ossrhPassword)
        }
    }
}
