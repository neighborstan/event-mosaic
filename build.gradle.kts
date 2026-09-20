plugins {
	java
	id("org.springframework.boot") version "4.1.0"
	id("io.spring.dependency-management") version "1.1.7"
	id("event-mosaic.frontend")
}

group = "com.neighbor"
version = "0.1.0-SNAPSHOT"
description = "Event Mosaic modular monolith"

val springModulithVersion = "2.1.0"

java {
	toolchain {
		languageVersion = JavaLanguageVersion.of(25)
	}
}

repositories {
	mavenCentral()
}

dependencies {
	// Application runtime
	implementation("org.springframework.boot:spring-boot-starter-actuator")
	implementation("org.springframework.boot:spring-boot-starter-validation")
	implementation("org.springframework.boot:spring-boot-starter-webmvc")

	// Persistence and migrations
	implementation("org.springframework.boot:spring-boot-starter-flyway")
	implementation("org.springframework.boot:spring-boot-starter-jdbc")
	implementation("org.flywaydb:flyway-database-postgresql")

	// Search read model
	implementation("org.springframework.boot:spring-boot-starter-elasticsearch")

	// Architecture contracts
	compileOnly(platform("org.springframework.modulith:spring-modulith-bom:$springModulithVersion"))
	compileOnly("org.springframework.modulith:spring-modulith-api")

	// Persistence runtime driver
	runtimeOnly("org.postgresql:postgresql")

	// Local development
	developmentOnly("org.springframework.boot:spring-boot-docker-compose")

	// Spring Boot test slices
	testImplementation("org.springframework.boot:spring-boot-starter-actuator-test")
	testImplementation("org.springframework.boot:spring-boot-starter-elasticsearch-test")
	testImplementation("org.springframework.boot:spring-boot-starter-flyway-test")
	testImplementation("org.springframework.boot:spring-boot-starter-jdbc-test")
	testImplementation("org.springframework.boot:spring-boot-starter-validation-test")
	testImplementation("org.springframework.boot:spring-boot-starter-webmvc-test")

	// Mockito для точечных unit tests без поддержки final/static/constructor mocks
	testImplementation("org.mockito:mockito-subclass")

	// Testcontainers
	testImplementation("org.springframework.boot:spring-boot-testcontainers")
	testImplementation("org.testcontainers:testcontainers-elasticsearch")
	testImplementation("org.testcontainers:testcontainers-junit-jupiter")
	testImplementation("org.testcontainers:testcontainers-postgresql")

	// Architecture verification
	testImplementation(platform("org.springframework.modulith:spring-modulith-bom:$springModulithVersion"))
	testImplementation("org.springframework.modulith:spring-modulith-starter-test")

	testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.withType<Test> {
	useJUnitPlatform()
	jvmArgs("-XX:+EnableDynamicAgentLoading")
}
