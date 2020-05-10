/*
 * Copyright (C) 2016-2017 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra

package object journal {
  def partitionNr(highestEventSequenceNr: Long, highestIdempotencyKeySequenceNr: Long, partitionSize: Long): Long = {
    //idempotency keys are written to two tables, one for search one for cache
    (highestEventSequenceNr + (highestIdempotencyKeySequenceNr * 2) - 1L) / partitionSize
  }
}
