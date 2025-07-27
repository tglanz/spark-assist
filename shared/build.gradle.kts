plugins {
    scala
}

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    implementation(libs.scala.library)
}

tasks.named<Test>("test") {
    useJUnitPlatform()
}
