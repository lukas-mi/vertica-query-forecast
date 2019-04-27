package org.mikelionis.lukas.vqf
package query

case class Column(table: Table, columnName: String) {
  val fqColumnName: String = List(table.schemaName, table.tableName, columnName).mkString(".")
  override def toString: String = fqColumnName
}

object Column {
  def apply(fqcn: String): Column = {
    val parts = fqcn.split(QueryUtils.FQ_SEPARATOR)

    require(parts.size == 3, "Column name has to be fully qualified <schema>.<table>.<column>")

    new Column(Table(parts(0), parts(1)), parts(2))
  }
}
