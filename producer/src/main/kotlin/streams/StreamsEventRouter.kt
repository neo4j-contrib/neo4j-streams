package streams

import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.Producer
import org.neo4j.logging.Log
import streams.events.EntityType
import streams.events.StreamsEvent
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.errors.AuthorizationException
import org.apache.kafka.common.errors.OutOfOrderSequenceException
import org.apache.kafka.common.errors.ProducerFencedException
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import streams.serialization.JacksonUtil
import java.lang.Exception


interface StreamsEventRouter {

    fun sendEvent(event : StreamsEvent)

}


class StreamsEventRouterImpl(private val log: Log, private val producer: Producer<Long, ByteArray>, private val config: StreamsConfiguration): StreamsEventRouter {

    init {
        producer.initTransactions()
    }

    private val producerCallback = { meta: RecordMetadata?, error: Exception? -> log.warn("sending record in partition ${meta?.partition()} offset ${meta?.offset()} data ${meta?.topic()} key size ${meta?.serializedKeySize()}", error) }

    override fun sendEvent(event: StreamsEvent) {
        val events = when (event.payload.type) {
            EntityType.node -> NodeRoutingConfiguration.prepareEvent(event, config.nodeRouting)
            EntityType.relationship -> RelationshipRoutingConfiguration.prepareEvent(event, config.relRouting)
        }
        try {
            log.debug("Trying to send the event with txId ${event.meta.txId} to kafka")
            producer.beginTransaction()
            events.forEach {
                val producerRecord = ProducerRecord(it.key, config.partitionSize, System.currentTimeMillis(), it.value.payload.id,
                        JacksonUtil.getMapper().writeValueAsBytes(it))
                producer.send(producerRecord, producerCallback)
            }
            producer.commitTransaction()
            log.debug("Event with txId ${event.meta.txId} sent successfully")
        } catch (e: ProducerFencedException) {
            log.error("Error:", e)
            producer.close()
        } catch (e: OutOfOrderSequenceException) {
            log.error("Error:", e)
            producer.close()
        } catch (e: AuthorizationException) {
            log.error("Error:", e)
            producer.close()
        } catch (e: KafkaException) {
            log.error("Error:", e)
            producer.abortTransaction()
        }
    }

}