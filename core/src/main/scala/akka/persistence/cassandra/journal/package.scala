/*
 * Copyright (C) 2016-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence.cassandra

import akka.annotation.InternalApi

package object journal {

  /** INTERNAL API */
  @InternalApi private[akka] def partitionNr(
      highestEventSequenceNr: Long,
      highestIdempotencyKeySequenceNr: Long,
      partitionSize: Long): Long = {
    //idempotency keys are written to two tables, one for search one for cache
    (highestEventSequenceNr + (highestIdempotencyKeySequenceNr * 2) - 1L) / partitionSize
  }
}
