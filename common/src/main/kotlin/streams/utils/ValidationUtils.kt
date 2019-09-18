package streams.utils

import java.io.IOException
import java.net.Socket
import java.net.URI

object ValidationUtils {

    fun isServerReachable(url: String, port: Int): Boolean = try {
        Socket(url, port).use { true }
    } catch (e: IOException) {
        false
    }

    fun checkServersUnreachable(urls: String, separator: String = ","): List<String> = urls
            .split(separator)
            .map {
                val uri = URI.create(it)
                when (uri.host.isNullOrBlank()) {
                    true -> {
                        val splitted = it.split(":")
                        URI("fake-scheme", "", splitted.first(), splitted.last().toInt(),
                                "", "", "")
                    }
                    else -> uri
                }
            }
            .filter { uri -> !isServerReachable(uri.host, uri.port) }
            .map { if (it.scheme == "fake-scheme") "${it.host}:${it.port}" else it.toString() }

    fun validateConnection(url: String, kafkaPropertyKey: String, checkReachable: Boolean = true) {
        if (url.isBlank()) {
            throw RuntimeException("The `kafka.$kafkaPropertyKey` property is empty")
        } else if (checkReachable) {
            val unreachableServers = checkServersUnreachable(url)
            if (unreachableServers.isNotEmpty()) {
                throw RuntimeException("The servers defined into the property `kafka.$kafkaPropertyKey` are not reachable: $unreachableServers")
            }
        }
    }

}