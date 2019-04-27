package org.mikelionis.lukas.vqf
package clustering

import query._
import TestsHelpers._

import org.scalatest.{FunSpec, Matchers}

class QueryClusterSimilarityTest extends FunSpec with Matchers {
  private val columns1 = List(
    ColumnWithClause(Column("s1.t1.c1"), Clause.SELECT),
    ColumnWithClause(Column("s1.t1.c1"), Clause.GROUPBY),
    ColumnWithClause(Column("s1.t1.c2"), Clause.SELECT),
    ColumnWithClause(Column("s1.t1.c2"), Clause.WHERE)
  )
  private val columns2 = List(
    ColumnWithClause(Column("s1.t1.c1"), Clause.SELECT),
    ColumnWithClause(Column("s1.t1.c1"), Clause.GROUPBY),
    ColumnWithClause(Column("s1.t1.c2"), Clause.SELECT)
  )
  private val query1 = makeQuery(columns1)
  private val query2 = makeQuery(columns2)
  private val cluster1 = List(query1, query2)

  it("should ignore query in similarity/dissimilarity calculation if it's passed as Query") {
    val (sim1, dis1) = measureBoth(query1, cluster1, queryClusterSimilarityMethod())
    val (sim2, dis2) = measureBoth(query1.analysed, query2.analysed, aligonMethod())

    sim1 should be > 0.0
    sim1 should be < 1.0
    dis1 should be > 0.0
    dis1 should be < 1.0

    sim1 shouldBe sim2
    dis1 shouldBe dis2
  }

  it("should not ignore query in similarity/dissimilarity calculation if it's passed as AnalysedQuery") {
    val (sim1, dis1) = measureBoth(query1.analysed, cluster1, queryClusterSimilarityMethod())
    val (sim2, dis2) = measureBoth(query1.analysed, query2.analysed, aligonMethod())

    sim1 should be > 0.0
    sim1 should be < 1.0
    dis1 should be > 0.0
    dis1 should be < 1.0

    sim2 should be > 0.0
    sim2 should be < 1.0
    dis2 should be > 0.0
    dis2 should be < 1.0

    sim1 should be > sim2
    dis1 should be < dis2
  }
}
