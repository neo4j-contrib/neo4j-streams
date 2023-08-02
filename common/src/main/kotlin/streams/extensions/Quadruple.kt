package streams.extensions

import java.io.Serializable

data class Quadruple<out A, out B, out C, out D>(
    val first: A,
    val second: B,
    val third: C,
    val forth: D
) : Serializable {
    override fun toString(): String = "($first, $second, $third, $forth)"
}
