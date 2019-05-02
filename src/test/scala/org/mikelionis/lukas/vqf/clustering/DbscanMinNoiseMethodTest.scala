package org.mikelionis.lukas.vqf
package clustering

import query._
import TestsHelpers._

import org.scalatest.{FunSpec, Matchers}

import scala.util.Random

class DbscanMinNoiseMethodTest extends FunSpec with Matchers {
  it("should minimize noise and include noise cluster, which should be the first cluster, when 'withNoise' is true") {
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
        ColumnWithClause(Column("s1.t1.c2"), Clause.SELECT),
        ColumnWithClause(Column("s1.t1.c2"), Clause.WHERE)
      )
    )
    val query3 = makeQuery(
      List(
        ColumnWithClause(Column("s1.t1.c1"), Clause.SELECT),
        ColumnWithClause(Column("s1.t1.c1"), Clause.GROUPBY),
        ColumnWithClause(Column("s1.t2.c2"), Clause.SELECT),
        ColumnWithClause(Column("s1.t2.c2"), Clause.WHERE)
      )
    )
    val query4 = makeQuery(
      List(
        ColumnWithClause(Column("s1.t1.c1"), Clause.SELECT),
        ColumnWithClause(Column("s1.t1.c1"), Clause.GROUPBY),
        ColumnWithClause(Column("s1.t3.c2"), Clause.SELECT),
        ColumnWithClause(Column("s1.t3.c2"), Clause.WHERE)
      )
    )
    val query5 = makeQuery(
      List(
        ColumnWithClause(Column("s1.t4.c1"), Clause.SELECT),
        ColumnWithClause(Column("s1.t4.c1"), Clause.GROUPBY),
        ColumnWithClause(Column("s1.t4.c2"), Clause.SELECT),
        ColumnWithClause(Column("s1.t4.c2"), Clause.WHERE)
      )
    )
    val queries = Random.shuffle(List(query1, query2, query3, query4, query5))

    // eps 0.1 - require high similarity
    val dbscan = new DbscanMethod(0.1, 1, true, aligonMethod())
    val dbscanClusters = dbscan.clusterQueries(queries)
    val expectedDbscanClusters = Vector(List(query1, query2), List(query3, query4, query5))
    val expectedDbscanNoisyCluster = List(query3, query4, query5)

    dbscanClusters.map(_.toSet) should contain theSameElementsAs expectedDbscanClusters.map(_.toSet)
    dbscanClusters.head should contain theSameElementsAs expectedDbscanNoisyCluster

    val dbscanMinNoise = new DbscanMinNoiseMethod(0.1, 1.2, 1, true, aligonMethod())
    val dbscanMinNoiseClusters = dbscanMinNoise.clusterQueries(queries)
    val expectedDbscanMinNoiseClusters = Vector(List(query1, query2), List(query3, query4), List(query5))
    val expectedDbscanMinNoiseNoisyCluster = List(query5)

    dbscanMinNoiseClusters.map(_.toSet) should contain theSameElementsAs expectedDbscanMinNoiseClusters.map(_.toSet)
    dbscanMinNoiseClusters.head should contain theSameElementsAs expectedDbscanMinNoiseNoisyCluster
  }

  it("should minimize noise and exclude noise cluster when 'withNoise' is true") {
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
        ColumnWithClause(Column("s1.t1.c2"), Clause.SELECT),
        ColumnWithClause(Column("s1.t1.c2"), Clause.WHERE)
      )
    )
    val query3 = makeQuery(
      List(
        ColumnWithClause(Column("s1.t1.c1"), Clause.SELECT),
        ColumnWithClause(Column("s1.t1.c1"), Clause.GROUPBY),
        ColumnWithClause(Column("s1.t2.c2"), Clause.SELECT),
        ColumnWithClause(Column("s1.t2.c2"), Clause.WHERE)
      )
    )
    val query4 = makeQuery(
      List(
        ColumnWithClause(Column("s1.t1.c1"), Clause.SELECT),
        ColumnWithClause(Column("s1.t1.c1"), Clause.GROUPBY),
        ColumnWithClause(Column("s1.t3.c2"), Clause.SELECT),
        ColumnWithClause(Column("s1.t3.c2"), Clause.WHERE)
      )
    )
    val query5 = makeQuery(
      List(
        ColumnWithClause(Column("s1.t4.c1"), Clause.SELECT),
        ColumnWithClause(Column("s1.t4.c1"), Clause.GROUPBY),
        ColumnWithClause(Column("s1.t4.c2"), Clause.SELECT),
        ColumnWithClause(Column("s1.t4.c2"), Clause.WHERE)
      )
    )
    val queries = Random.shuffle(List(query1, query2, query3, query4, query5))

    val dbscanMinNoise = new DbscanMinNoiseMethod(0.1, 1.2, 1, false, aligonMethod())
    val dbscanMinNoiseClusters = dbscanMinNoise.clusterQueries(queries)
    val expectedDbscanMinNoiseClusters = Vector(List(query1, query2), List(query3, query4))

    dbscanMinNoiseClusters.map(_.toSet) should contain theSameElementsAs expectedDbscanMinNoiseClusters.map(_.toSet)
  }
}
