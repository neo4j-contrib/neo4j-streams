package streams.procedures

data class StreamPublishResult(
        @JvmField public var topic: String,
        @JvmField public var payload: Any,
        @JvmField public var config: Map<String,Any>?,
        @JvmField public var timestamp: Long?,
        @JvmField public var offset: Long?,
        @JvmField public var partition: Long?,
        @JvmField public var keySize: Long?,
        @JvmField public var valueSize: Long?
)