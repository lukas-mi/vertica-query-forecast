package org.mikelionis.lukas.vqf
package query

case class Table(schemaName: String, tableName: String) {
  val fqTableName: String = List(schemaName, tableName).mkString(".")
  override def toString: String = fqTableName
}

object Table {
  def apply(fqtn: String): Table = {
    val parts = fqtn.split(QueryUtils.FQ_SEPARATOR)

    require(parts.size == 2, "Table name has to be fully qualified <schema>.<table>")

    new Table(parts(0), parts(1))
  }
}
