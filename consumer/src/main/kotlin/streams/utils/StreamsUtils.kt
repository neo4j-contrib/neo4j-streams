package streams.utils

object StreamsUtils {

    fun <T> ignoreExceptions(action: () -> T, vararg toIgnore: Class<out Throwable>): T? {
        return try {
            action()
        } catch (e: Throwable) {
            when (e::class.java) {
                in toIgnore -> null
                else -> throw e
            }
        }
    }

}