package streams

import org.slf4j.Logger
import java.io.BufferedReader
import java.io.File
import java.io.InputStreamReader


object MavenUtils {
    fun mvnw(path: String = ".", logger: Logger? = null, vararg args: String) {

        val rt = Runtime.getRuntime()
        val mvnw = if (System.getProperty("os.name").startsWith("Windows")) "./mvnw.cmd" else "./mvnw"
        val commands = arrayOf(mvnw, "-pl", "!doc,!kafka-connect-neo4j", "-DbuildSubDirectory=containerPlugins") +
                args.let { if (it.isNullOrEmpty()) arrayOf("package", "-Dmaven.test.skip") else it }
        val proc = rt.exec(commands, null, File(path))

        val stdInput = BufferedReader(InputStreamReader(proc.inputStream))

        val stdError = BufferedReader(InputStreamReader(proc.errorStream))

        // Read the output from the command
        var s: String? = null
        while (stdInput.readLine().also { s = it } != null) {
            logger?.info(s)
        }

        // Read any errors from the attempted command
        while (stdError.readLine().also { s = it } != null) {
            logger?.error(s)
        }
    }
}