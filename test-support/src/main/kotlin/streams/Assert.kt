package streams

import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration
import org.hamcrest.Matcher
import org.hamcrest.StringDescription
import org.neo4j.function.ThrowingSupplier
import java.lang.invoke.MethodType
import java.util.Locale
import java.util.concurrent.TimeUnit
import java.util.function.BiPredicate
import kotlin.math.abs

object Assert {
    fun <T, E : java.lang.Exception?> assertEventually(
        actual: ThrowingSupplier<T, E>,
        matcher: Matcher<in T>,
        timeout: Long,
        timeUnit: TimeUnit
    ) {
        assertEventually({ _: T -> "" }, actual, matcher, timeout, timeUnit)
    }

    fun <T, E : java.lang.Exception?> assertEventually(
        reason: String,
        actual: ThrowingSupplier<T, E>,
        matcher: Matcher<in T>,
        timeout: Long,
        timeUnit: TimeUnit
    ) {
        assertEventually({ _: T -> reason }, actual, matcher, timeout, timeUnit)
    }

    fun <T, E : java.lang.Exception?> assertEventually(
        reason: java.util.function.Function<T, String>,
        actual: ThrowingSupplier<T, E>,
        matcher: Matcher<in T>,
        timeout: Long,
        timeUnit: TimeUnit
    ) {
        val endTimeMillis = System.currentTimeMillis() + timeUnit.toMillis(timeout)
        while (true) {
            val sampleTime = System.currentTimeMillis()
            val last: T = actual.get()
            val matched: Boolean = matcher.matches(last)
            if (matched || sampleTime > endTimeMillis) {
                if (!matched) {
                    val description = StringDescription()
                    description.appendText(reason.apply(last)).appendText("\nExpected: ").appendDescriptionOf(matcher)
                        .appendText("\n     but: ")
                    matcher.describeMismatch(last, description)
                    throw AssertionError(
                        "Timeout hit (" + timeout + " " + timeUnit.toString()
                            .lowercase(Locale.ROOT) + ") while waiting for condition to match: " + description.toString()
                    )
                } else {
                    return
                }
            }
            Thread.sleep(100L)
        }
    }

    fun withDoublePrecision(precision: Double = 0.00001): RecursiveComparisonConfiguration {
        val areClose = BiPredicate<Double, Double> { d1, d2 ->
            abs(d1 - d2) < precision
        }

        return RecursiveComparisonConfiguration.builder()
            .withEqualsForType<Double>(areClose, Double::class.java)
            .withEqualsForType<Double>(
                areClose,
                MethodType.methodType(Double::class.java).wrap().returnType() as Class<Double>
            )
            .build()
    }
}