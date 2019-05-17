package org.mikelionis.lukas.vqf
package similarity

import query._

case class Features(
    projection: Set[Column],
    selection: Set[Column],
    groupBy: Set[Column]
) {
  override def toString: String = {
    val p = projection.mkString(", ")
    val s = selection.mkString(", ")
    val g = groupBy.mkString(", ")
    s"F(p={$p}, s={$s}, g={$g})"
  }
}

object Features {
  def apply(query: AnalysedQuery): Features = {
    val groupedColumns = query.columns.groupBy { case ColumnWithClause(_, clause) => clause }

    def extractColumns(clause: Clause): Set[Column] = groupedColumns.getOrElse(clause, Set.empty).map(_.column)

    val projectionColumns = extractColumns(Clause.SELECT)
    val selectionColumns = extractColumns(Clause.WHERE) | extractColumns(Clause.JOIN) | extractColumns(Clause.HAVING)
    val groupByColumns = extractColumns(Clause.GROUPBY)

    new Features(projectionColumns, selectionColumns, groupByColumns)
  }
}
