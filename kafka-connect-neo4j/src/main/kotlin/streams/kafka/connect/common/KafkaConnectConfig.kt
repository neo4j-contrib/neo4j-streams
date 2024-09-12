package streams.kafka.connect.common

object KafkaConnectConfig {

  val options = setOf(
    // common
    "name",
    "connector.class",
    "tasks.max",
    "key.converter",
    "value.converter",
    "header.converter",
    "config.action.reload",
    "transforms",
    "predicates",
    "errors.retry.timeout",
    "errors.retry.delay.max.ms",
    "errors.tolerance",
    "errors.log.enable",
    "errors.log.include.messages",
    // sink
    "topics",
    "topic.regex",
    "errors.deadletterqueue.topic.name",
    "errors.deadletterqueue.topic.replication.factor",
    "errors.deadletterqueue.context.headers.enable",
    // source
    "topic.creation.groups",
    "exactly.once.support",
    "transaction.boundary",
    "transaction.boundary.interval.ms",
    "offsets.storage.topic"
  )

}
