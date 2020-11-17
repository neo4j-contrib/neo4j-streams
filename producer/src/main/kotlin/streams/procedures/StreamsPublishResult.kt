package streams.procedures

data class StreamPublishResult(
        @JvmField var topic: String,
        @JvmField var payload: Any,
        @JvmField var config: Map<String, Any>?,
        @JvmField var timestamp: Long?,
        @JvmField var offset: Long?,
        @JvmField var partition: Long?,
        @JvmField var keySize: Long?,
        @JvmField var valueSize: Long?
)