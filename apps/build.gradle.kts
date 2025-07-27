plugins {
    scala
    id("com.github.johnrengelman.shadow")
}

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)

    implementation(libs.scala.library)

    compileOnly(libs.spark)
    implementation(libs.delta)

    implementation(project(":shared"))
}

tasks.named<Test>("test") {
    useJUnitPlatform()
}

tasks.named("build") {
    dependsOn("shadowJar")
}