import java.io.File

plugins {
	java
}

val frontendDirectory = layout.projectDirectory.dir("frontend")
val frontendOutput = layout.buildDirectory.dir("generated-resources/frontend/static")
val isWindows = System.getProperty("os.name").startsWith("Windows")
val localNodeDirectory = layout.projectDirectory.dir(".local/node-v24.19.0-win-x64").asFile
	.takeIf { isWindows && it.isDirectory }
val nodeCommand = listOf(localNodeDirectory?.resolve("node.exe")?.absolutePath ?: "node")
val npmCommand = if (localNodeDirectory != null) {
	nodeCommand + localNodeDirectory.resolve("node_modules/npm/bin/npm-cli.js").absolutePath
} else if (isWindows) {
	listOf("cmd", "/d", "/c", "npm")
} else {
	listOf("npm")
}
// npm scripts также должны находить выбранный node, а не случайную системную версию.
val frontendEnvironment = localNodeDirectory?.let {
	mapOf("PATH" to "${it.absolutePath}${File.pathSeparator}${System.getenv("PATH").orEmpty()}")
}.orEmpty()
val runtimeHelp = "Подготовьте Node.js 24.19.0 и npm 11.19.0 в .local/node-v24.19.0-win-x64 на Windows или в PATH по README.md."

val checkFrontendRuntime by tasks.registering {
	group = "build"
	description = "Проверяет установленный Node.js/npm перед сборкой frontend."
	doLast {
		for ((command, expected) in listOf(nodeCommand to "v24.19.0", npmCommand to "11.19.0")) {
			val actual = try {
				providers.exec {
					commandLine(command + "--version")
					environment(frontendEnvironment)
				}.standardOutput.asText.get().trim()
			} catch (exception: Exception) {
				throw GradleException("Не удалось запустить выбранный Node.js/npm. $runtimeHelp", exception)
			}
			check(actual == expected) {
				"Для frontend требуется Node.js 24.19.0 и npm 11.19.0: ожидалось $expected, найдено $actual. $runtimeHelp"
			}
		}
	}
}

val frontendInstall by tasks.registering(Exec::class) {
	group = "build"
	description = "Устанавливает frontend-зависимости из точного lockfile."
	dependsOn(checkFrontendRuntime)
	workingDir(frontendDirectory)
	environment(frontendEnvironment)
	commandLine(npmCommand + listOf("ci", "--include=dev", "--no-audit", "--no-fund"))
	inputs.files(frontendDirectory.file("package.json"), frontendDirectory.file("package-lock.json"), frontendDirectory.file(".npmrc"))
	outputs.dir(frontendDirectory.dir("node_modules"))
	val installMarker = layout.buildDirectory.file("frontend/install-complete")
	outputs.file(installMarker)
	doLast {
		installMarker.get().asFile.apply {
			parentFile.mkdirs()
			writeText("Node.js 24.19.0 / npm 11.19.0\n")
		}
	}
}

val frontendBuild by tasks.registering(Exec::class) {
	group = "build"
	description = "Собирает frontend в generated resources Spring Boot."
	dependsOn(frontendInstall)
	workingDir(frontendDirectory)
	environment(frontendEnvironment)
	commandLine(npmCommand + listOf("run", "build", "--", "--outDir", frontendOutput.get().asFile.absolutePath, "--emptyOutDir"))
	inputs.files(fileTree(frontendDirectory) {
		exclude("node_modules/**", "dist/**", "coverage/**")
	})
	inputs.property("viteEnvironment", providers.environmentVariablesPrefixedBy("VITE_"))
	inputs.property("nodeEnvironment", providers.environmentVariable("NODE_ENV").orElse("production"))
	outputs.dir(frontendOutput)
}

tasks.processResources {
	from(frontendBuild) { into("static") }
	// Убираем старые hashed assets при изменении bundle; исходная геометрия копируется заново.
	doFirst { delete(destinationDir) }
}
