package org.mikelionis.lukas.vqf
package clustering

import query._
import TestsHelpers._

import org.scalatest.{FunSpec, Matchers}

import scala.util.Random

class DbscanTest extends FunSpec with Matchers {
  it("should cluster queries and include noise cluster, which should be the first cluster, when 'withNoise' is true") {
    val query1 = makeQuery(
      List(
        ColumnWithClause(Column("s1.t1.c1"), Clause.SELECT),
        ColumnWithClause(Column("s1.t1.c1"), Clause.GROUPBY),
        ColumnWithClause(Column("s1.t1.c2"), Clause.SELECT),
        ColumnWithClause(Column("s1.t1.c2"), Clause.WHERE)
      )
    )
    val query2 = makeQuery(
      List(
        ColumnWithClause(Column("s1.t1.c1"), Clause.SELECT),
        ColumnWithClause(Column("s1.t1.c1"), Clause.GROUPBY),
        ColumnWithClause(Column("s1.t1.c2"), Clause.SELECT)
      )
    )
    val query3 = makeQuery(
      List(
        ColumnWithClause(Column("s1.t2.c1"), Clause.SELECT),
        ColumnWithClause(Column("s1.t2.c1"), Clause.GROUPBY),
        ColumnWithClause(Column("s1.t2.c2"), Clause.SELECT),
        ColumnWithClause(Column("s1.t2.c2"), Clause.WHERE)
      )
    )
    val query4 = makeQuery(
      List(
        ColumnWithClause(Column("s1.t2.c1"), Clause.SELECT),
        ColumnWithClause(Column("s1.t2.c1"), Clause.GROUPBY),
        ColumnWithClause(Column("s1.t2.c2"), Clause.SELECT)
      )
    )
    val query5 = makeQuery(
      List(
        ColumnWithClause(Column("s1.t3.c1"), Clause.SELECT),
        ColumnWithClause(Column("s1.t3.c1"), Clause.GROUPBY),
        ColumnWithClause(Column("s1.t3.c2"), Clause.SELECT),
        ColumnWithClause(Column("s1.t3.c2"), Clause.WHERE)
      )
    )
    val queries = List(query1, query2, query3, query4, query5)

    val dbscan = new DbscanMethod(0.5, 1, true, aligonMethod())
    val clusters = dbscan.clusterQueries(Random.shuffle(queries))
    val expectedClusters = Vector(List(query1, query2), List(query3, query4), List(query5))
    val expectedNoisyCluster = List(query5)

    clusters.map(_.toSet) should contain theSameElementsAs expectedClusters.map(_.toSet)
    clusters.head shouldBe expectedNoisyCluster
  }

  it("should cluster queries and exclude noise cluster when 'withNoise' is false") {
    val query1 = makeQuery(
      List(
        ColumnWithClause(Column("s1.t1.c1"), Clause.SELECT),
        ColumnWithClause(Column("s1.t1.c1"), Clause.GROUPBY),
        ColumnWithClause(Column("s1.t1.c2"), Clause.SELECT),
        ColumnWithClause(Column("s1.t1.c2"), Clause.WHERE)
      )
    )
    val query2 = makeQuery(
      List(
        ColumnWithClause(Column("s1.t1.c1"), Clause.SELECT),
        ColumnWithClause(Column("s1.t1.c1"), Clause.GROUPBY),
        ColumnWithClause(Column("s1.t1.c2"), Clause.SELECT)
      )
    )
    val query3 = makeQuery(
      List(
        ColumnWithClause(Column("s1.t2.c1"), Clause.SELECT),
        ColumnWithClause(Column("s1.t2.c1"), Clause.GROUPBY),
        ColumnWithClause(Column("s1.t2.c2"), Clause.SELECT),
        ColumnWithClause(Column("s1.t2.c2"), Clause.WHERE)
      )
    )
    val query4 = makeQuery(
      List(
        ColumnWithClause(Column("s1.t2.c1"), Clause.SELECT),
        ColumnWithClause(Column("s1.t2.c1"), Clause.GROUPBY),
        ColumnWithClause(Column("s1.t2.c2"), Clause.SELECT)
      )
    )
    val query5 = makeQuery(
      List(
        ColumnWithClause(Column("s1.t3.c1"), Clause.SELECT),
        ColumnWithClause(Column("s1.t3.c1"), Clause.GROUPBY),
        ColumnWithClause(Column("s1.t3.c2"), Clause.SELECT),
        ColumnWithClause(Column("s1.t3.c2"), Clause.WHERE)
      )
    )
    val queries = Random.shuffle(List(query1, query2, query3, query4, query5))

    val dbscan = new DbscanMethod(0.5, 1, false, aligonMethod())
    val clusters = dbscan.clusterQueries(queries)
    val expectedClusters = Vector(List(query1, query2), List(query3, query4))

    clusters.map(_.toSet) should contain theSameElementsAs expectedClusters.map(_.toSet)
  }
}
