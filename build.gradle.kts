val junitJupiterVersion = "5.12.1"
val postgresqlVersion = "42.7.5"
val tbdLibsVersion = "2025.03.30-14.11-a91ce546"
val rapidsAndRiversVersion = "2025061811051750237542.df739400e55e"
val mockkVersion = "1.13.17"
val hikariCPVersion = "6.3.0"
val flywayVersion = "11.5.0"

plugins {
    kotlin("jvm") version "2.1.20"
}

repositories {
    val githubPassword: String? by project
    mavenCentral()
    /* ihht. https://github.com/navikt/utvikling/blob/main/docs/teknisk/Konsumere%20biblioteker%20fra%20Github%20Package%20Registry.md
        så plasseres github-maven-repo (med autentisering) før nav-mirror slik at github actions kan anvende førstnevnte.
        Det er fordi nav-mirroret kjører i Google Cloud og da ville man ellers fått unødvendige utgifter til datatrafikk mellom Google Cloud og GitHub
     */
    maven {
        url = uri("https://maven.pkg.github.com/navikt/maven-release")
        credentials {
            username = "x-access-token"
            password = githubPassword
        }
    }
    maven("https://github-package-registry-mirror.gc.nav.no/cached/maven-release")
}

dependencies {
    implementation("org.postgresql:postgresql:$postgresqlVersion")
    implementation("com.github.navikt:rapids-and-rivers:$rapidsAndRiversVersion")
    implementation("com.github.navikt.tbd-libs:spurtedu-client:$tbdLibsVersion")
    implementation("com.github.navikt.tbd-libs:azure-token-client-default:$tbdLibsVersion")
    implementation("com.github.navikt.tbd-libs:retry:$tbdLibsVersion")
    implementation("com.github.navikt.tbd-libs:speed-client:$tbdLibsVersion")
    implementation("com.github.navikt.tbd-libs:spedisjon-client:$tbdLibsVersion")

    implementation("com.zaxxer:HikariCP:$hikariCPVersion")
    implementation("org.flywaydb:flyway-database-postgresql:$flywayVersion")
    implementation("com.github.seratch:kotliquery:1.9.0")

    testImplementation("org.junit.jupiter:junit-jupiter:$junitJupiterVersion")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    testImplementation("io.mockk:mockk:$mockkVersion")
    testImplementation("com.github.navikt.tbd-libs:rapids-and-rivers-test:$tbdLibsVersion")
    testImplementation("com.github.navikt.tbd-libs:postgres-testdatabaser:$tbdLibsVersion")
}

kotlin {
    jvmToolchain {
        languageVersion.set(JavaLanguageVersion.of("21"))
    }
}

tasks {

    named<Jar>("jar") {
        archiveBaseName.set("app")

        manifest {
            attributes["Main-Class"] = "no.nav.helse.AppKt"
            attributes["Class-Path"] = configurations.runtimeClasspath.get().joinToString(separator = " ") {
                it.name
            }
        }

        doLast {
            configurations.runtimeClasspath.get().forEach {
                val file = File("${layout.buildDirectory.get()}/libs/${it.name}")
                if (!file.exists()) it.copyTo(file)
            }
        }
    }

    withType<Test> {
        useJUnitPlatform()
        testLogging {
            events("passed", "skipped", "failed")
        }
    }
}
