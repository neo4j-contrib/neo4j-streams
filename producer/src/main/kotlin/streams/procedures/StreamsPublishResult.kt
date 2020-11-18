package streams.procedures

data class StreamPublishResult(
        @JvmField val topic: String,
        @JvmField val payload: Any,
        @JvmField val config: Map<String, Any>?,
        @JvmField var timestamp: Long,
        @JvmField val offset: Long,
        @JvmField val partition: Long,
        @JvmField val keySize: Long,
        @JvmField val valueSize: Long
)