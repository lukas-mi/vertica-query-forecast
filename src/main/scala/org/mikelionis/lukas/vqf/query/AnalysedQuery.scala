package org.mikelionis.lukas.vqf
package query

case class AnalysedQuery(
    statement: Statement,
    tables: Set[Table],
    lineage: Set[Lineage],
    columns: Set[ColumnWithClause],
    joins: Set[Join]
)
