package streams.utils

import org.junit.Test
import java.io.IOException
import java.lang.RuntimeException
import kotlin.test.assertNull
import kotlin.test.assertTrue

class StreamsUtilsTest {

    private val foo = "foo"

    @Test
    fun shouldReturnValue() {
        val data = StreamsUtils.ignoreExceptions({
            foo
        }, RuntimeException::class.java)
        assertTrue { data != null && data == foo }
    }

    @Test
    fun shouldIgnoreTheException() {
        val data = StreamsUtils.ignoreExceptions({
            throw RuntimeException()
        }, RuntimeException::class.java)
        assertNull(data)
    }

    @Test(expected = IOException::class)
    fun shouldNotIgnoreTheException() {
        StreamsUtils.ignoreExceptions({
            throw IOException()
        }, RuntimeException::class.java)
    }
}