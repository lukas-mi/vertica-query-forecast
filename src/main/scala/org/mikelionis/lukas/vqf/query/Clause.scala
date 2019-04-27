package org.mikelionis.lukas.vqf
package query

sealed trait Clause extends Ordered[Clause] {
  override def toString: String = Clause.toString(this)
  override def compare(that: Clause): Int = Clause.toInt(this).compare(Clause.toInt(that))
}

object Clause {
  case object SELECT extends Clause
  case object JOIN extends Clause
  case object WHERE extends Clause
  case object GROUPBY extends Clause
  case object HAVING extends Clause
  case object ORDERBY extends Clause
  case object PARTITIONBY extends Clause

  def toString(clause: Clause): String = clause match {
    case SELECT => "SELECT"
    case JOIN => "JOIN"
    case WHERE => "WHERE"
    case GROUPBY => "GROUPBY"
    case HAVING => "HAVING"
    case ORDERBY => "ORDER"
    case PARTITIONBY => "PARTITION"
  }

  def fromString(clause: String): Clause = clause match {
    case "SELECT" => SELECT
    case "JOIN" => JOIN
    case "WHERE" => WHERE
    case "GROUPBY" => GROUPBY
    case "HAVING" => HAVING
    case "ORDER" => ORDERBY
    case "PARTITION" => PARTITIONBY
  }

  private def toInt(clause: Clause): Int = clause match {
    case SELECT => 0
    case JOIN => 1
    case WHERE => 2
    case GROUPBY => 3
    case HAVING => 4
    case ORDERBY => 5
    case PARTITIONBY => 6
  }

}
