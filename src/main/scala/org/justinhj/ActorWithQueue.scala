import zio.{App, Queue, UIO, ZIO}
import zio.console._
import akka.actor._
import com.typesafe.config.ConfigFactory
import java.util.concurrent.Executors

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import java.util.concurrent.{Executors, TimeUnit}
import java.util.concurrent.{ExecutorService, Executors, ThreadFactory}

import zio.blocking.Blocking

object ActorWithQueue extends App {

  // Messages
  case object Subscribe
  case object Publish
  case class Message(payload: String)

  // Actor that will send period messages to subscribers
  class Publisher() extends Actor {

    import context._

    val scheduler = context.system.scheduler

    var subscribers = Set.empty[ActorRef]

    override def preStart(): Unit =
      scheduler.scheduleOnce(FiniteDuration(5, TimeUnit.SECONDS), self, Publish)

    def receive = {
      case Subscribe =>
        subscribers = subscribers + sender()
      case Publish =>
        val payload = s"""{"data" : "${System.currentTimeMillis()}"}"""
        for (subscriber <- subscribers) subscriber ! Message(payload)

        scheduler.scheduleOnce(FiniteDuration(1, TimeUnit.SECONDS), self, Publish)
    }

  }

  // Actor subscribes to publisher and pushes incoming messages to the Zio queue
  class Subscriber(publisher: ActorRef, queue: Queue[String]) extends Actor {
    override def preStart() = publisher ! Subscribe

    def receive = {
      case Message(payload) =>
        //println(s"Received: $payload")
        val offer: UIO[Boolean] = queue.offer(payload)
        unsafeRun(offer)

    }
  }

  val config = ConfigFactory.load()

  def run(args: List[String]) = {

    def processElement(q: Queue[String]) =
      q.take.flatMap { payload =>
        putStrLn(s"Processing queue payload: $payload")
      }

    val myAppLogic =
      for {
        blockingEc <- ZIO.environment[Blocking].flatMap(_.blocking.blockingExecutor).map(_.asEC)
        as = ActorSystem("actorz", defaultExecutionContext = Some(blockingEc), config = Some(config))
        publishActor = as.actorOf(Props(new Publisher), "publisher")
        q <- Queue.bounded[String](5);
        _ = as.actorOf(Props(new Subscriber(publishActor, q)), "sub-1")
        child <- processElement(q).forever.fork
        _ <- child.join
      } yield ()

    myAppLogic.fold(_ => 1, _ => 0)
  }
}
