package streams.utils

import org.junit.Test
import java.lang.RuntimeException
import kotlin.test.assertTrue

class StreamsUtilsTest {

    private val foo = "foo"

    @Test
    fun shouldReturnValue() {
        val data = StreamsUtils.ignoreExceptions({
            foo
        }, RuntimeException::class.java)
        assertTrue { data != null && data!! == foo }
    }

    @Test
    fun shouldIgnoreTheException() {
        val data = StreamsUtils.ignoreExceptions({
            throw RuntimeException()
        }, RuntimeException::class.java)
        assertTrue { data == null }
    }

    @Test(expected = IllegalArgumentException::class)
    fun shouldNotIgnoreTheException() {
        val data = StreamsUtils.ignoreExceptions({
            throw IllegalArgumentException()
        }, RuntimeException::class.java)
    }
}