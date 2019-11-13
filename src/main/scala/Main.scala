import java.util.UUID

import org.apache.kafka.clients.producer.ProducerRecord
import sttp.client._
import sttp.client.asynchttpclient.zio.AsyncHttpClientZioBackend
import sttp.model.{MediaType, Uri}
import zio._
import zio.clock.Clock
import zio.blocking.Blocking
import zio.system.System
import zio.duration._
import zio.kafka.client.serde.Serde
import zio.kafka.client.{CommittableRecord, Consumer, ConsumerSettings, Offset, OffsetBatch, Producer, ProducerSettings, Subscription}

case class Config(bootstrapServer: String, kafkaTopicIn: String, kafkaTopicOut: String, mlUrl: Uri)

object Main extends App {
  /** This is the type of chunk we'll be writing to Kafka. */
  type MyChunk = (org.apache.kafka.clients.producer.ProducerRecord[String,String], zio.kafka.client.Offset)

  override def run(args: List[String]): ZIO[ZEnv, Nothing, Int] = {
    wsToKafka.fold(_ => 1, _ => 0)
  }

  val config: ZIO[System, Throwable, Config] = for {
    bootstrapServer <- system.env("BOOTSTRAP_SERVER").someOrFail(new IllegalArgumentException("Could not find bootstrap server config."))
    kafkaTopicIn <- system.env("KAFKA_TOPIC_IN").someOrFail(new IllegalArgumentException("Could not find kafka input topic."))
    kafkaTopicOut <- system.env("KAFKA_TOPIC_OUT").someOrFail(new IllegalArgumentException("Could not find kafka output topic."))
    rawUri <- system.env("ML_URL").someOrFail(new IllegalArgumentException("Could not find URL for ML server."))
    mlUrl <- ZIO.fromTry(Uri.parse(rawUri))
  } yield Config(bootstrapServer, kafkaTopicIn, kafkaTopicOut, mlUrl)

  // todo: exit doesn't work
  val wsToKafka: ZIO[ZEnv, Throwable, Unit] = 
    for {
      c <- config
      result <- runMlService(c)
    } yield result

  /** Given config, this will open Kafka channels and handle events, firing requests to the ML service and updated the queue. */
  def runMlService(config: Config): ZIO[ZEnv, Throwable, Unit] = {
    val producerSettings = ProducerSettings(
      bootstrapServers = List(config.bootstrapServer),
      closeTimeout = 30.seconds,
      extraDriverSettings = Map.empty,
    )

    val consumerSettings = ConsumerSettings(
      bootstrapServers = List(config.bootstrapServer),
      groupId = "zio-kafka-ml",
      clientId = UUID.randomUUID().toString,
      closeTimeout = 30.seconds,
      extraDriverSettings = Map(),
      pollInterval = 250.millis,
      pollTimeout = 50.millis,
      perPartitionChunkPrefetch = 2
    )

    val subscription = Subscription.topics(config.kafkaTopicIn)
    // Constructs the ML component in the ZIO environment.  We can use this to "provideSomeM" away our dependencies
    // in the chain.
    val makeMlService: ZIO[ZEnv, Throwable, ZEnv with ML] = 
      for {
        env <- ZIO.environment[ZEnv]
        result <- ML.default(config.mlUrl)(env)
      } yield result

    // todo: seekToEnd
    (Consumer.make(consumerSettings) zip Producer.make(producerSettings, Serde.string, Serde.string)).use { 
      case (consumer, producer) =>
        //  A stream of string->string records
        consumer.subscribeAnd(subscription).plainStream(Serde.string, Serde.string)
        // Send each record through our business logic
        .mapM(handleInputRecord(config))
        // Provide the ML Service to run the business logic. (As we did not have the ML module)
        .provideSomeM(makeMlService)
        // Peel of chunks and commit them into kafka
        .chunks.mapM(commitChunk(producer))
        // Drain the entire queue
        .runDrain
    }
  }

  /** This takes a chunk and sets it for commit. */
  def commitChunk[Env](producer: Producer[Env, String, String])(chunk: zio.Chunk[MyChunk]): ZIO[Env with Blocking, Throwable, Unit] = {
    val records = chunk.map(_._1)
    val offsetBatch = OffsetBatch(chunk.map(_._2).toSeq)
    producer.produceChunk(records) *> offsetBatch.commit
  }

  /** This function takes a record from kafka and updates it with ML results. */
  def handleInputRecord(config: Config)(record: CommittableRecord[String, String]): ZIO[ZEnv with ML, Throwable, MyChunk] = {
    /*
      ML Service must receive:
      {
        "instances": [
          {
            "end_station_id": "333",
            "ts": 1435774380.0,
            "day_of_week": "4",
            "start_stujsonation_id": "160",
            "euclidean": 4295.88,
            "loc_cross": "POINT(-0.13 51.51)POINT(-0.19 51.51)",
            "prcp": 0.0,
            "max": 94.5,
            "min": 58.9,
            "temp": 81.8,
            "dewp": 59.5
          }
        ]
      }
    */

    val in = ujson.read(record.record.value())
    val mlRequest = ujson.Obj(
      "instances" -> ujson.Arr(
        ujson.Obj(
          "end_station_id" -> in("end_station_id"),
          "ts" -> in("ts"),
          "day_of_week" -> in("day_of_week"),
          "start_station_id" -> in("start_station_id"),
          "euclidean" -> in("euclidean"),
          "loc_cross" -> in("loc_cross"),
          "prcp" -> in("prcp"),
          "max" -> in("max"),
          "min" -> in("min"),
          "temp" -> in("temp"),
          "dewp" -> in("dewp"),
        )
      )
    )
    for {
      resp <- ML.send(mlRequest)
    } yield {
      // Update the incoming kafka record with preductions, then create a Chunk update with that value.
      in.update("prediction", resp("predictions")(0)(0).num)
      val producerRecord = new ProducerRecord(config.kafkaTopicOut, record.record.key, in.toString())
      (producerRecord, record.offset)
    }
  }
}
