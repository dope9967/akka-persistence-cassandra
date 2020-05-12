package akka.persistence.cassandra.journal

import java.util.UUID

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.{ ActorRef, Behavior }
import akka.persistence.cassandra.{ CassandraLifecycle, CassandraSpec }
import akka.persistence.typed.scaladsl._
import akka.persistence.typed.{ CheckIdempotencyKeyExistsSucceeded, PersistenceId, WriteIdempotencyKeySucceeded }
import com.typesafe.config.{ Config, ConfigFactory }
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration._

object AbstractCassandraIdempotenceCacheSpec {

  private sealed trait Command
  private case class SideEffect(
      override val idempotencyKey: String,
      override val replyTo: ActorRef[IdempotenceReply[AllGood.type, Int]])
      extends Command
      with IdempotentCommand[AllGood.type, Int]

  private object NoSideEffect {
    case class WriteAlways(
        override val idempotencyKey: String,
        override val replyTo: ActorRef[IdempotenceReply[AllGood.type, Int]])
        extends Command
        with IdempotentCommand[AllGood.type, Int]

    case class WriteOnlyWithPersist(
        override val idempotencyKey: String,
        override val replyTo: ActorRef[IdempotenceReply[AllGood.type, Int]])
        extends Command
        with IdempotentCommand[AllGood.type, Int] {
      override val writeConfig: IdempotenceKeyWriteConfig = OnlyWriteIdempotenceKeyWithPersist
    }
  }
  private case class CurrentIdempotencyKeyCacheContent(replyTo: ActorRef[Seq[String]]) extends Command

  private case object AllGood

  private def idempotentState(
      persistenceId: String,
      checks: ActorRef[String],
      writes: ActorRef[String]): Behavior[Command] =
    Behaviors.setup { ctx =>
      EventSourcedBehavior
        .withEnforcedReplies[Command, Int, Int](
          PersistenceId.ofUniqueId(persistenceId),
          emptyState = 0,
          commandHandler = (_, command) => {
            command match {
              case SideEffect(_, replyTo) =>
                Effect.persist(1).thenReply(replyTo)(_ => IdempotenceSuccess(AllGood))
              case NoSideEffect.WriteAlways(_, replyTo) =>
                Effect.none[Int, Int].thenReply(replyTo)(_ => IdempotenceSuccess(AllGood))
              case NoSideEffect.WriteOnlyWithPersist(_, replyTo) =>
                Effect.none[Int, Int].thenReply(replyTo)(_ => IdempotenceSuccess(AllGood))
              case CurrentIdempotencyKeyCacheContent(replyTo) =>
                Effect.none[Int, Int].thenReply(replyTo)(_ => EventSourcedBehavior.idempotencyKeyCacheContent(ctx))
            }
          },
          eventHandler = (state, event) => {
            state + event
          })
        .receiveSignal {
          case (_, CheckIdempotencyKeyExistsSucceeded(idempotencyKey, exists)) =>
            checks ! s"$idempotencyKey ${if (exists) "exists" else "not exists"}"
          case (_, WriteIdempotencyKeySucceeded(idempotencyKey, sequenceNumber)) =>
            writes ! s"$idempotencyKey $sequenceNumber written"
        }
        .idempotenceKeyCacheSize(3)
    }
}

class CassandraIdempotenceCacheSpec
    extends AbstractCassandraIdempotenceCacheSpec(
      ConfigFactory.parseString("""
                                |akka.loglevel = DEBUG
                                |cassandra-journal.write-static-column-compat = off
                                |""".stripMargin).withFallback(CassandraLifecycle.config))

class CassandraIdempotenceCacheSpecSmallPartition
    extends AbstractCassandraIdempotenceCacheSpec(
      ConfigFactory.parseString("""
                                |akka.loglevel = DEBUG
                                |cassandra-journal.write-static-column-compat = off
                                |cassandra-journal.target-partition-size = 3
                                |""".stripMargin).withFallback(CassandraLifecycle.config))

abstract class AbstractCassandraIdempotenceCacheSpec(config: Config)
    extends CassandraSpec(config)
    with AnyWordSpecLike
    with Matchers {
  import AbstractCassandraIdempotenceCacheSpec._

  private val testKit = ActorTestKit("CassandraIdempotenceSpec")

  override protected def afterAll(): Unit = {
    testKit.shutdownTestKit()
    super.afterAll()
  }

  "side-effecting idempotent command" should {
    "cache idempotency keys" in {
      val checksProbe = testKit.createTestProbe[String]()
      val writesProbe = testKit.createTestProbe[String]()

      val probe = testKit.createTestProbe[IdempotenceReply[AllGood.type, Int]]
      val idempotencyKeyCacheProbe = testKit.createTestProbe[Seq[String]]

      val persistenceId = nextPid
      val idempotenceKey = UUID.randomUUID().toString

      val c = system.spawnAnonymous(idempotentState(persistenceId, checksProbe.ref, writesProbe.ref))

      c ! SideEffect(idempotenceKey, probe.ref)
      checksProbe.expectMessage(s"$idempotenceKey not exists")
      writesProbe.expectMessage(s"$idempotenceKey 1 written")

      c ! CurrentIdempotencyKeyCacheContent(idempotencyKeyCacheProbe.ref)
      idempotencyKeyCacheProbe.expectMessage(Seq(idempotenceKey))

      c ! SideEffect(idempotenceKey, probe.ref)
      checksProbe.expectNoMessage(3.seconds)
      writesProbe.expectNoMessage(3.seconds)

      c ! CurrentIdempotencyKeyCacheContent(idempotencyKeyCacheProbe.ref)
      idempotencyKeyCacheProbe.expectMessage(Seq(idempotenceKey))
    }
  }

  "not side-effecting idempotent command if key should always write" should {
    "cache idempotency keys" in {
      val checksProbe = testKit.createTestProbe[String]()
      val writesProbe = testKit.createTestProbe[String]()

      val probe = testKit.createTestProbe[IdempotenceReply[AllGood.type, Int]]
      val idempotencyKeyCacheProbe = testKit.createTestProbe[Seq[String]]

      val persistenceId = nextPid
      val idempotenceKey = UUID.randomUUID().toString

      val c = system.spawnAnonymous(idempotentState(persistenceId, checksProbe.ref, writesProbe.ref))

      c ! NoSideEffect.WriteAlways(idempotenceKey, probe.ref)
      checksProbe.expectMessage(s"$idempotenceKey not exists")
      writesProbe.expectMessage(s"$idempotenceKey 1 written")

      c ! CurrentIdempotencyKeyCacheContent(idempotencyKeyCacheProbe.ref)
      idempotencyKeyCacheProbe.expectMessage(Seq(idempotenceKey))

      c ! NoSideEffect.WriteAlways(idempotenceKey, probe.ref)
      checksProbe.expectNoMessage(3.seconds)
      writesProbe.expectNoMessage(3.seconds)

      c ! CurrentIdempotencyKeyCacheContent(idempotencyKeyCacheProbe.ref)
      idempotencyKeyCacheProbe.expectMessage(Seq(idempotenceKey))
    }
  }

  "LRU cache" should {
    "cache and restore" in {
      val checksProbe = testKit.createTestProbe[String]()
      val writesProbe = testKit.createTestProbe[String]()

      val probe = testKit.createTestProbe[IdempotenceReply[AllGood.type, Int]]
      val idempotencyKeyCacheProbe = testKit.createTestProbe[Seq[String]]

      val persistenceId = nextPid

      val c = system.spawnAnonymous(idempotentState(persistenceId, checksProbe.ref, writesProbe.ref))

      c ! SideEffect("k1", probe.ref)
      checksProbe.expectMessage(s"k1 not exists")
      writesProbe.expectMessage(s"k1 1 written")
      c ! SideEffect("k2", probe.ref)
      checksProbe.expectMessage(s"k2 not exists")
      writesProbe.expectMessage(s"k2 2 written")
      c ! SideEffect("k3", probe.ref)
      checksProbe.expectMessage(s"k3 not exists")
      writesProbe.expectMessage(s"k3 3 written")
      c ! SideEffect("k4", probe.ref)
      checksProbe.expectMessage(s"k4 not exists")
      writesProbe.expectMessage(s"k4 4 written")

      c ! CurrentIdempotencyKeyCacheContent(idempotencyKeyCacheProbe.ref)
      idempotencyKeyCacheProbe.expectMessage(Seq("k2", "k3", "k4"))

      c ! SideEffect("k3", probe.ref)
      c ! CurrentIdempotencyKeyCacheContent(idempotencyKeyCacheProbe.ref)
      idempotencyKeyCacheProbe.expectMessage(Seq("k2", "k4", "k3"))

      val r = system.spawnAnonymous(idempotentState(persistenceId, checksProbe.ref, writesProbe.ref))

      Thread.sleep(500)

      r ! CurrentIdempotencyKeyCacheContent(idempotencyKeyCacheProbe.ref)
      idempotencyKeyCacheProbe.expectMessage(Seq("k2", "k3", "k4"))
    }
  }
}
