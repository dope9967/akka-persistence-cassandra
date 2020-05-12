/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import java.util.UUID

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.scaladsl.adapter._
import akka.persistence.cassandra.{ CassandraLifecycle, CassandraSpec }
import akka.persistence.typed.scaladsl._
import akka.persistence.typed.{ CheckIdempotencyKeyExistsSucceeded, PersistenceId, WriteIdempotencyKeySucceeded }
import com.typesafe.config.{ Config, ConfigFactory }
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration._

object AbstractCassandraIdempotenceSpec {

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

  private case object AllGood

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
              Effect.persist(1).thenReply(replyTo)(_ => IdempotenceSuccess(AllGood))
            case NoSideEffect.WriteAlways(_, replyTo) =>
              Effect.none[Int, Int].thenReply(replyTo)(_ => IdempotenceSuccess(AllGood))
            case NoSideEffect.WriteOnlyWithPersist(_, replyTo) =>
              Effect.none[Int, Int].thenReply(replyTo)(_ => IdempotenceSuccess(AllGood))
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
}

class CassandraIdempotenceSpec
    extends AbstractCassandraIdempotenceSpec(
      ConfigFactory.parseString("""
                              |akka.loglevel = DEBUG
                              |cassandra-journal.write-static-column-compat = off
                              |""".stripMargin).withFallback(CassandraLifecycle.config))

class CassandraIdempotenceSpecSmallPartition
    extends AbstractCassandraIdempotenceSpec(
      ConfigFactory.parseString("""
                                |akka.loglevel = DEBUG
                                |cassandra-journal.write-static-column-compat = off
                                |cassandra-journal.target-partition-size = 3
                                |""".stripMargin).withFallback(CassandraLifecycle.config))

abstract class AbstractCassandraIdempotenceSpec(config: Config)
    extends CassandraSpec(config)
    with AnyWordSpecLike
    with Matchers {
  import AbstractCassandraIdempotenceSpec._

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
      val probe = testKit.createTestProbe[IdempotenceReply[AllGood.type, Int]]

      c ! SideEffect(idempotenceKey, probe.ref)
      probe.expectMessage(IdempotenceSuccess[AllGood.type, Int](AllGood))
      checksProbe.expectMessage(s"$idempotenceKey not exists")
      writesProbe.expectMessage(s"$idempotenceKey 1 written")
    }

    "restore highest sequence number on replay" in {
      val checksProbe = testKit.createTestProbe[String]()
      val writesProbe = testKit.createTestProbe[String]()

      val persistenceId = nextPid
      val c1 = system.spawnAnonymous(idempotentState(persistenceId, checksProbe.ref, writesProbe.ref))
      val probe = testKit.createTestProbe[IdempotenceReply[AllGood.type, Int]]

      (1 to 10).foreach { i =>
        val idempotenceKey = UUID.randomUUID().toString
        c1 ! SideEffect(idempotenceKey, probe.ref)
        probe.expectMessage(IdempotenceSuccess[AllGood.type, Int](AllGood))
        checksProbe.expectMessage(s"$idempotenceKey not exists")
        writesProbe.expectMessage(s"$idempotenceKey $i written")
      }

      val idempotenceKey = UUID.randomUUID().toString
      val c2 = system.spawnAnonymous(idempotentState(persistenceId, checksProbe.ref, writesProbe.ref))
      c2 ! SideEffect(idempotenceKey, probe.ref)
      checksProbe.expectMessage(s"$idempotenceKey not exists")
      writesProbe.expectMessage(s"$idempotenceKey 11 written")
    }

    "fail consume idempotent command the second time" in {
      val checksProbe = testKit.createTestProbe[String]()
      val writesProbe = testKit.createTestProbe[String]()

      val idempotenceKey = UUID.randomUUID().toString
      val c = system.spawnAnonymous(idempotentState(nextPid, checksProbe.ref, writesProbe.ref))
      val probe = testKit.createTestProbe[IdempotenceReply[AllGood.type, Int]]

      c ! SideEffect(idempotenceKey, probe.ref)
      probe.expectMessage(IdempotenceSuccess[AllGood.type, Int](AllGood))
      checksProbe.expectMessage(s"$idempotenceKey not exists")
      writesProbe.expectMessage(s"$idempotenceKey 1 written")

      c ! SideEffect(idempotenceKey, probe.ref)
      probe.expectMessage(IdempotenceFailure[AllGood.type, Int](1))
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
      val probe = testKit.createTestProbe[IdempotenceReply[AllGood.type, Int]]

      c ! NoSideEffect.WriteAlways(idempotenceKey, probe.ref)
      probe.expectMessage(IdempotenceSuccess[AllGood.type, Int](AllGood))
      checksProbe.expectMessage(s"$idempotenceKey not exists")
      writesProbe.expectMessage(s"$idempotenceKey 1 written")

      c ! NoSideEffect.WriteAlways(idempotenceKey, probe.ref)
      probe.expectMessage(IdempotenceFailure[AllGood.type, Int](0))
      checksProbe.expectMessage(s"$idempotenceKey exists")
      writesProbe.expectNoMessage(3.seconds)
    }

    "succeed consume the second time if key should write only with persist" in {
      val checksProbe = testKit.createTestProbe[String]()
      val writesProbe = testKit.createTestProbe[String]()

      val idempotenceKey = UUID.randomUUID().toString
      val c = system.spawnAnonymous(idempotentState(nextPid, checksProbe.ref, writesProbe.ref))
      val probe = testKit.createTestProbe[IdempotenceReply[AllGood.type, Int]]

      c ! NoSideEffect.WriteOnlyWithPersist(idempotenceKey, probe.ref)
      probe.expectMessage(IdempotenceSuccess[AllGood.type, Int](AllGood))
      checksProbe.expectMessage(s"$idempotenceKey not exists")
      writesProbe.expectNoMessage(3.seconds)

      c ! NoSideEffect.WriteOnlyWithPersist(idempotenceKey, probe.ref)
      probe.expectMessage(IdempotenceSuccess[AllGood.type, Int](AllGood))
      checksProbe.expectMessage(s"$idempotenceKey not exists")
      writesProbe.expectNoMessage(3.seconds)
    }
  }
}
