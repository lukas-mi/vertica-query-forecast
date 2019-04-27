package org.mikelionis.lukas.vqf
package query

import java.time.{Duration, LocalDateTime}

case class QueryMetadata(user: String, epoch: Int, session: String, start: LocalDateTime, duration: Duration)
