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

  val config: ZIO[System, Any, Config] = for {
    bootstrapServer <- system.env("BOOTSTRAP_SERVER").someOrFail()
    kafkaTopicIn <- system.env("KAFKA_TOPIC_IN").someOrFail()
    kafkaTopicOut <- system.env("KAFKA_TOPIC_OUT").someOrFail()
    mlUrl <- system.env("ML_URL").someOrFail().flatMap { mlUri => ZIO.fromTry(Uri.parse(mlUri)) }
  } yield Config(bootstrapServer, kafkaTopicIn, kafkaTopicOut, mlUrl)

  // todo: exit doesn't work
  val wsToKafka: ZIO[ZEnv, Any, Unit] = 
    for {
      c <- config
      result <- runMlService(c)
    } yield result

  /** Given config, this will open Kafka channels and handle events, firing requests to the ML service and updated the queue. */
  def runMlService(config: Config): ZIO[ZEnv, Any, Unit] = {
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

    // todo: seekToEnd
    AsyncHttpClientZioBackend().flatMap { implicit sttpBackend =>
      (Consumer.make(consumerSettings) zip Producer.make(producerSettings, Serde.string, Serde.string)).use { 
        case (consumer, producer) =>
          consumer.subscribeAnd(subscription).plainStream(Serde.string, Serde.string)
          .mapM(handleInputRecord(config, sttpBackend))
          .chunks.mapM(commitChunk(producer))
          .runDrain
      }
    }
  }

  /** This takes a chunk and sets it for commit. */
  def commitChunk[Env](producer: Producer[Env, String, String])(chunk: zio.Chunk[MyChunk]): ZIO[Env with Blocking, Any, Unit] = {
    val records = chunk.map(_._1)
    val offsetBatch = OffsetBatch(chunk.map(_._2).toSeq)
    producer.produceChunk(records) *> offsetBatch.commit
  }

  /** This function takes an incoming Kafka Consumer record and creates a ProducerRecord (at the same offset) that will write the ML result to that record. */
  def handleInputRecord(config: Config, backend: sttp.client.SttpBackend[zio.Task,Nothing,sttp.client.asynchttpclient.WebSocketHandler])(
    record: CommittableRecord[String,String]): ZIO[Any, Any, MyChunk] = {
    /*
      ML Service must receive:
      {
        "instances": [
          {
            "end_station_id": "333",
            "ts": 1435774380.0,
            "day_of_week": "4",
            "start_station_id": "160",
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

    val toMl = ujson.Obj(
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
      response <-  basicRequest.post(config.mlUrl).body(toMl.toString()).contentType(MediaType.ApplicationJson).send()(backend, implicitly)
      chunk <- handleMlResponse(config.kafkaTopicOut, record.record.key, in, record.offset)(response)
    } yield chunk
  }


  /* Take the respnse from calling the ML service and create a new Kafka record for it. */
  def handleMlResponse(kafkaTopicOut: String, 
                       recordKey: String,
                       mlJson: ujson.Value.Value,
                       offset: Offset)(response: Response[Either[String,String]]): ZIO[Any, String, MyChunk] =
    ZIO.fromEither(response.body).map { body =>

      val fromMl = ujson.read(body)
      /*
      {
        "predictions": [[1501.77026]]
      }
       */

       mlJson.update("prediction", fromMl("predictions")(0)(0).num)

      val producerRecord = new ProducerRecord(kafkaTopicOut, recordKey, mlJson.toString())
      (producerRecord, offset)
    }
}
