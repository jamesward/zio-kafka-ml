import java.util.UUID

import org.apache.kafka.clients.producer.ProducerRecord
import sttp.client._
import sttp.client.asynchttpclient.zio.AsyncHttpClientZioBackend
import sttp.model.{MediaType, Uri}
import zio._
import zio.system.System
import zio.random.Random
import zio.console.Console
import zio.clock.Clock
import zio.blocking.Blocking
import zio.system.System
import zio.duration._
import zio.kafka.client.serde.Serde
import zio.kafka.client.{CommittableRecord, Consumer, ConsumerSettings, Offset, OffsetBatch, Producer, ProducerSettings, Subscription}

/** Creates a module for composing in our ML webserivce. */
trait ML extends Serializable {
  def ml: ML.Service[Any]
}
/** The static access for the ML module.   This has constructions + access methods of the API. */
object ML {
  /** This is the interface we use to access the webservice hooks for ML. */
  trait Service[R] {
    /** Sends a JSON value to the ML service and gets a response. */
    def send(json: ujson.Value.Value): ZIO[R, Throwable, ujson.Value.Value]
  }

  /** Helper method to access the ML module on the environment and send/receive a json value. */
  def send(json: ujson.Value.Value): ZIO[ZEnv with ML, Throwable, ujson.Value.Value] =
    ZIO.accessM[ML](_.ml.send(json))


  /** Constructs a default webservice endpoint using AsyncHttpClient.   This will use the async client to send all json. */
  def default(uri: Uri)(env: ZEnv): ZIO[Any, Throwable, ZEnv with ML] =
    AsyncHttpClientZioBackend().map { implicit sttpBackend =>
      new Clock with Console with System with Random with Blocking with ML {
        override val clock = env.clock
        override val console = env.console
        override val system = env.system
        override val random = env.random
        override val blocking = env.blocking
        object ml extends ML.Service[Any] {
          override def send(json: ujson.Value.Value): ZIO[Any, Throwable, ujson.Value.Value] =
            for {
              response <- basicRequest.post(uri).body(json.toString()).contentType(MediaType.ApplicationJson).send()
              body <- ZIO.fromEither(response.body.left.map(err => throw new RuntimeException(err)))
            } yield ujson.read(body)
        }
      }
    }




}