package streams.utils

import org.junit.Ignore
import org.junit.Test
import org.testcontainers.containers.GenericContainer
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class FakeWebServer: GenericContainer<FakeWebServer>("alpine") {
    override fun start() {
        this.withCommand("/bin/sh", "-c", "while true; do { echo -e 'HTTP/1.1 200 OK'; echo ; } | nc -l -p 8000; done")
                .withExposedPorts(8000)
        super.start()
    }

    fun getUrl() = "http://localhost:${getMappedPort(8000)}"
}

@Ignore("fails on CI")
class ValidationUtilsTest {

    @Test
    fun `should reach the server`() {
        val httpServer = FakeWebServer()
        httpServer.start()
        assertTrue { ValidationUtils.checkServersUnreachable(httpServer.getUrl()).isEmpty() }
        httpServer.stop()
    }

    @Test
    fun `should not reach the server`() {
        val urls = "http://my.fake.host:1234,PLAINTEXT://my.fake.host1:1234,my.fake.host2:1234"
        val checkServersUnreachable = ValidationUtils
                .checkServersUnreachable(urls)
        assertTrue { checkServersUnreachable.isNotEmpty() }
        assertEquals(urls.split(",").toList(), checkServersUnreachable)
    }
}