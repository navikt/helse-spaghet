plugins {
    alias(libs.plugins.sykepenger.deployable)
}

sykepengerDeployable {
    mainClass = "no.nav.helse.AppKt"
}

dependencies {
    implementation(libs.postgresql)
    implementation(libs.rapidsAndRivers)
    implementation(libs.tbdLibs.spurteduClient)
    implementation(libs.tbdLibs.azureTokenClientDefault)
    implementation(libs.tbdLibs.retry)
    implementation(libs.tbdLibs.speedClient)
    implementation(libs.tbdLibs.spedisjonClient)

    implementation(libs.hikari)
    implementation(libs.flyway.postgresql)
    implementation(libs.kotliquery)
    implementation(libs.sykepengerLibs.logging)

    testImplementation(libs.mockk)
    testImplementation(libs.tbdLibs.rapidsAndRiversTest)
    testImplementation(libs.tbdLibs.postgresTestdatabaser)
}
