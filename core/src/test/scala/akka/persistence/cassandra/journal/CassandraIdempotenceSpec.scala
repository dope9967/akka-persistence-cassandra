/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import java.util.UUID

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.adapter._
import akka.persistence.cassandra.CassandraSpec
import akka.persistence.typed.scaladsl._
import akka.persistence.typed.{ CheckIdempotencyKeyExistsSucceeded, PersistenceId, WriteIdempotencyKeySucceeded }
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration._

object CassandraIdempotenceSpec {

  private sealed trait Command
  private case class SideEffect(override val idempotencyKey: String, override val replyTo: ActorRef[IdempotenceReply])
      extends Command
      with IdempotentCommand

  private object NoSideEffect {
    case class WriteAlways(override val idempotencyKey: String, override val replyTo: ActorRef[IdempotenceReply])
        extends Command
        with IdempotentCommand

    case class WriteOnlyWithPersist(
        override val idempotencyKey: String,
        override val replyTo: ActorRef[IdempotenceReply])
        extends Command
        with IdempotentCommand {
      override val writeConfig: IdempotenceKeyWriteConfig = OnlyWriteIdempotenceKeyWithPersist
    }
  }

  private case object AllGood extends IdempotenceReply

  private def idempotentState(
      persistenceId: String,
      checks: ActorRef[String],
      writes: ActorRef[String]): EventSourcedBehavior[Command, Int, Int] =
    EventSourcedBehavior
      .withEnforcedReplies[Command, Int, Int](
        PersistenceId.ofUniqueId(persistenceId),
        emptyState = 0,
        commandHandler = (_, command) => {
          command match {
            case SideEffect(_, replyTo) =>
              Effect.persist(1).thenReply(replyTo)(_ => AllGood)
            case NoSideEffect.WriteAlways(_, replyTo) =>
              Effect.none[Int, Int].thenReply(replyTo)(_ => AllGood)
            case NoSideEffect.WriteOnlyWithPersist(_, replyTo) =>
              Effect.none[Int, Int].thenReply(replyTo)(_ => AllGood)
          }
        },
        eventHandler = (state, event) => {
          state + event
        })
      .receiveSignal {
        case (_, CheckIdempotencyKeyExistsSucceeded(idempotencyKey, exists)) =>
          checks ! s"$idempotencyKey ${if (exists) "exists" else "not exists"}"
        case (_, WriteIdempotencyKeySucceeded(idempotencyKey)) =>
          writes ! s"$idempotencyKey written"
      }
}

class CassandraIdempotenceSpec extends CassandraSpec(dumpRowsOnFailure = false) with AnyWordSpecLike with Matchers {

  import CassandraIdempotenceSpec._

  private val testKit = ActorTestKit("CassandraIdempotenceSpec")

  override protected def afterAll(): Unit = {
    testKit.shutdownTestKit()
    super.afterAll()
  }

  "side-effecting idempotent command" should {
    "consume idempotent command" in {
      val checksProbe = testKit.createTestProbe[String]()
      val writesProbe = testKit.createTestProbe[String]()

      val idempotenceKey = UUID.randomUUID().toString
      val c = system.spawnAnonymous(idempotentState(nextPid, checksProbe.ref, writesProbe.ref))
      val probe = testKit.createTestProbe[IdempotenceReply]

      c ! SideEffect(idempotenceKey, probe.ref)
      probe.expectMessage(AllGood)
      checksProbe.expectMessage(s"$idempotenceKey not exists")
      writesProbe.expectMessage(s"$idempotenceKey written")
    }

    "fail consume idempotent command the second time" in {
      val checksProbe = testKit.createTestProbe[String]()
      val writesProbe = testKit.createTestProbe[String]()

      val idempotenceKey = UUID.randomUUID().toString
      val c = system.spawnAnonymous(idempotentState(nextPid, checksProbe.ref, writesProbe.ref))
      val probe = testKit.createTestProbe[IdempotenceReply]

      c ! SideEffect(idempotenceKey, probe.ref)
      probe.expectMessage(AllGood)
      checksProbe.expectMessage(s"$idempotenceKey not exists")
      writesProbe.expectMessage(s"$idempotenceKey written")

      c ! SideEffect(idempotenceKey, probe.ref)
      probe.expectMessage(IdempotenceFailure)
      checksProbe.expectMessage(s"$idempotenceKey exists")
      writesProbe.expectNoMessage(3.seconds)
    }
  }

  "not side-effecting idempotent command" should {
    "fail consume the second time if key should always write" in {
      val checksProbe = testKit.createTestProbe[String]()
      val writesProbe = testKit.createTestProbe[String]()

      val idempotenceKey = UUID.randomUUID().toString
      val c = system.spawnAnonymous(idempotentState(nextPid, checksProbe.ref, writesProbe.ref))
      val probe = testKit.createTestProbe[IdempotenceReply]

      c ! NoSideEffect.WriteAlways(idempotenceKey, probe.ref)
      probe.expectMessage(AllGood)
      checksProbe.expectMessage(s"$idempotenceKey not exists")
      writesProbe.expectMessage(s"$idempotenceKey written")

      c ! NoSideEffect.WriteAlways(idempotenceKey, probe.ref)
      checksProbe.expectMessage(s"$idempotenceKey exists")
      writesProbe.expectNoMessage(3.seconds)
    }

    "succeed consume the second time if key should write only with persist" in {
      val checksProbe = testKit.createTestProbe[String]()
      val writesProbe = testKit.createTestProbe[String]()

      val idempotenceKey = UUID.randomUUID().toString
      val c = system.spawnAnonymous(idempotentState(nextPid, checksProbe.ref, writesProbe.ref))
      val probe = testKit.createTestProbe[IdempotenceReply]

      c ! NoSideEffect.WriteOnlyWithPersist(idempotenceKey, probe.ref)
      probe.expectMessage(AllGood)
      checksProbe.expectMessage(s"$idempotenceKey not exists")
      writesProbe.expectNoMessage(3.seconds)

      c ! NoSideEffect.WriteOnlyWithPersist(idempotenceKey, probe.ref)
      probe.expectMessage(AllGood)
      checksProbe.expectMessage(s"$idempotenceKey not exists")
      writesProbe.expectNoMessage(3.seconds)
    }
  }
}
