/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra.journal

import akka.Done
import akka.event.LoggingAdapter
import akka.persistence.cassandra.PluginSettings
import akka.persistence.cassandra.journal.CassandraJournal.{ Serialized, TagPidSequenceNr }
import com.datastax.oss.driver.api.core.cql.{ PreparedStatement, Row, Statement }

import scala.collection.JavaConverters._
import scala.concurrent.{ ExecutionContext, Future }
import java.lang.{ Long => JLong }

import akka.annotation.InternalApi
import akka.stream.alpakka.cassandra.scaladsl.CassandraSession

/** INTERNAL API */
@InternalApi private[akka] trait CassandraEventUpdate {

  private[akka] val session: CassandraSession
  private[akka] def settings: PluginSettings
  private[akka] implicit val ec: ExecutionContext
  private[akka] val log: LoggingAdapter

  private def journalSettings = settings.journalSettings
  private lazy val journalStatements = new CassandraJournalStatements(settings)
  def psUpdateMessage: Future[PreparedStatement] = session.prepare(journalStatements.updateMessagePayloadAndTags)
  def psSelectTagPidSequenceNr: Future[PreparedStatement] = session.prepare(journalStatements.selectTagPidSequenceNr)
  def psUpdateTagView: Future[PreparedStatement] = session.prepare(journalStatements.updateMessagePayloadInTagView)
  def psSelectMessages: Future[PreparedStatement] = session.prepare(journalStatements.selectMessages)

  /**
   * Update the given event in the messages table and the tag_views table.
   *
   * Does not support changing tags in anyway. The tags field is ignored.
   */
  def updateEvent(event: Serialized): Future[Done] =
    for {
      (partitionNr, existingTags) <- findEvent(event)
      psUM <- psUpdateMessage
      e = event.copy(tags = existingTags) // do not allow updating of tags
      _ <- session.executeWrite(prepareUpdate(psUM, e, partitionNr))
      _ <- Future.traverse(existingTags) { tag =>
        updateEventInTagViews(event, tag)
      }
    } yield Done

  private def findEvent(s: Serialized): Future[(Long, Set[String])] = {
    for {
      ps <- psSelectMessages
      row <- findEvent(ps, s.persistenceId, s.sequenceNr)
    } yield (row.getLong("partition_nr"), row.getSet[String]("tags", classOf[String]).asScala.toSet)
  }

  /**
   * Event partition cannot be determined from sequenceNr, but max partition
   * can be calculated by adding up maxSequenceNr of events and idempotency keys
   */
  //TODO add max partition bound
  private def findEvent(ps: PreparedStatement, pid: String, sequenceNr: Long): Future[Row] = {
    def scan(pNr: Long): Future[Row] = {
      session.selectOne(ps.bind(pid, pNr: JLong, sequenceNr: JLong, sequenceNr: JLong)).flatMap {
        case Some(row) => Future.successful(row)
        case None =>
          scan(pNr + 1)
      }
    }
    scan(0)
    //TODO throw something like this in case max partition bound is exceeded
//    throw new RuntimeException(
//      s"Unable to find event: Pid: [$pid] SequenceNr: [$sequenceNr]")
  }

  private def updateEventInTagViews(event: Serialized, tag: String): Future[Done] =
    psSelectTagPidSequenceNr
      .flatMap { ps =>
        val bound = ps
          .bind()
          .setString("tag_name", tag)
          .setLong("timebucket", event.timeBucket.key)
          .setUuid("timestamp", event.timeUuid)
          .setString("persistence_id", event.persistenceId)
        session.selectOne(bound)
      }
      .map {
        case Some(r) => r.getLong("tag_pid_sequence_nr")
        case None =>
          throw new RuntimeException(
            s"no tag pid sequence nr. Pid ${event.persistenceId}. Tag: $tag. SequenceNr: ${event.sequenceNr}")
      }
      .flatMap { tagPidSequenceNr =>
        updateEventInTagViews(event, tag, tagPidSequenceNr)
      }

  private def updateEventInTagViews(event: Serialized, tag: String, tagPidSequenceNr: TagPidSequenceNr): Future[Done] =
    psUpdateTagView.flatMap { ps =>
      // primary key
      val bound = ps
        .bind()
        .setString("tag_name", tag)
        .setLong("timebucket", event.timeBucket.key)
        .setUuid("timestamp", event.timeUuid)
        .setString("persistence_id", event.persistenceId)
        .setLong("tag_pid_sequence_nr", tagPidSequenceNr)
        .setByteBuffer("event", event.serialized)
        .setString("ser_manifest", event.serManifest)
        .setInt("ser_id", event.serId)
        .setString("event_manifest", event.eventAdapterManifest)

      session.executeWrite(bound)
    }

  private def prepareUpdate(ps: PreparedStatement, s: Serialized, partitionNr: Long): Statement[_] = {
    // primary key
    ps.bind()
      .setString("persistence_id", s.persistenceId)
      .setLong("partition_nr", partitionNr)
      .setLong("sequence_nr", s.sequenceNr)
      .setUuid("timestamp", s.timeUuid)
      .setString("timebucket", s.timeBucket.key.toString)
      .setInt("ser_id", s.serId)
      .setString("ser_manifest", s.serManifest)
      .setString("event_manifest", s.eventAdapterManifest)
      .setByteBuffer("event", s.serialized)
      .setSet("tags", s.tags.asJava, classOf[String])
  }
}
