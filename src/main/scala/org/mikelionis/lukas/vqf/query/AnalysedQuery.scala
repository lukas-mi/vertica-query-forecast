package org.mikelionis.lukas.vqf
package query

import similarity._

case class AnalysedQuery(
    statement: Statement,
    tables: Set[Table],
    lineage: Set[Lineage],
    columns: Set[ColumnWithClause],
    joins: Set[Join]
) {
  lazy val features: Features = Features(this)
}
