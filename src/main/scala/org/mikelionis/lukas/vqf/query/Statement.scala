package org.mikelionis.lukas.vqf
package query

// TODO: add remaining statements
sealed trait Statement {
  override def toString: String = Statement.toString(this)
}

object Statement {
  case object SELECT extends Statement

  def toString(stmt: Statement): String = stmt match {
    case SELECT => "SELECT"
  }

  def fromString(stmt: String): Statement = stmt match {
    case "SELECT" => SELECT
  }
}
