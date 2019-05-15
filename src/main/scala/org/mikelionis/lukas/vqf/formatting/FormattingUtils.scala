package org.mikelionis.lukas.vqf
package formatting

import query._

object FormattingUtils {
  def prettyColumns(columns: Iterable[ColumnWithClause]): String =
    columns
      .groupBy(_.clause)
      .toList
      .sortBy { case (clause, _) => clause }
      .map {
        case (clause, columnsWithClause) =>
          s"[$clause]{${columnsWithClause.map(_.column).toList.sortBy(_.fqColumnName).mkString(",")}}"
      }
      .mkString(", ")

  def prettyQuery(query: Query): String = prettyColumns(query.analysed.columns)

  def prettyQuery(query: AnalysedQuery): String = prettyColumns(query.columns)

  def prettyGroups(itemGroups: Seq[(String, Iterable[String])]): String =
    itemGroups
      .flatMap {
        case (group, items) =>
          group +: items.map("\t" + _).toArray
      }
      .mkString("\n")

  def prettyQueryGroups(queryGroups: Seq[(String, Iterable[Query])]): String =
    prettyGroups(
      queryGroups.map {
        case (group, queries) =>
          group -> queries.map(q => FormattingUtils.prettyColumns(q.analysed.columns))
      }
    )

  def prettyQueries(queryGroups: Seq[Iterable[Query]]): String =
    prettyQueryGroups {
      val indexedGroups = queryGroups.zipWithIndex.map(_.swap)
      indexedGroups.map { case (index, queries) => index.toString -> queries }
    }

  def prettyPairGroups(pairGroups: Seq[(String, Iterable[(Any, Any)])]): String =
    prettyGroups(pairGroups.map { case (group, items) => group -> items.map { case (i1, i2) => s"$i1 $i2" } })

  def prettyPairs(pairs: Iterable[(Any, Any)]): String = pairs.map { case (i1, i2) => s"$i1 $i2" }.mkString("\n")

  def prettyMatrix(matrix: Seq[Seq[Double]]): String = {
    val matrixHeader = "\t" + matrix.indices.map(i => FormattingUtils.toStringPad(i, 5)).mkString(", ")
    val matrixRows = matrix.zipWithIndex.map {
      case (row, index) => index.toString + "\t" + row.map(p => f"$p%.3f").mkString(", ")
    }
    (matrixHeader +: matrixRows).mkString("\n")
  }

  def toStringPad(item: Any, n: Int): String = item.toString.reverse.padTo(n, ' ').reverse
}
