package org.mikelionis.lukas.vqf
package model

import TestsHelpers._
import query._

import org.scalatest.{FunSpec, Matchers}

import scala.util.Random

class MarkovModelTest extends FunSpec with Matchers {
  it("should forecast most probable future queries") {
    // query1 is similar to query2, while query3 is similar to query4
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
        ColumnWithClause(Column("s1.t1.c1"), Clause.WHERE),
        ColumnWithClause(Column("s1.t1.c2"), Clause.SELECT),
        ColumnWithClause(Column("s1.t1.c2"), Clause.WHERE)
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
        ColumnWithClause(Column("s1.t2.c1"), Clause.WHERE),
        ColumnWithClause(Column("s1.t2.c2"), Clause.SELECT),
        ColumnWithClause(Column("s1.t2.c2"), Clause.WHERE)
      )
    )
    val cluster1 = Random.shuffle(List(query3, query1, query1, query1))
    val cluster2 = Random.shuffle(List(query2, query3, query3, query3))
    val cluster3 = Random.shuffle(List(query4, query2, query2, query2))
    val clusters = Vector(cluster1, cluster2, cluster3)

    val matrix = Vector(
      Vector(3.0, 7.0, 2.0),
      Vector(4.0, 3.0, 8.0),
      Vector(1.0, 0.0, 3.0)
    ).map(row => row.map(_ / row.sum))

    val model = new MarkovModel(MarkovModelData(clusters, matrix, aligonMethod()), 0.0, 0.0)

    model.forecastQueries(query1.analysed) shouldBe List(query2)
    model.forecastQueries(query3.analysed) shouldBe List(query4)
  }

  // https://en.wikipedia.org/wiki/Absorbing_Markov_chain
  it("should not forecast future query when given query belongs to a cluster of absorbing state") {
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
        ColumnWithClause(Column("s1.t2.c1"), Clause.SELECT),
        ColumnWithClause(Column("s1.t2.c1"), Clause.GROUPBY),
        ColumnWithClause(Column("s1.t2.c2"), Clause.SELECT),
        ColumnWithClause(Column("s1.t2.c2"), Clause.WHERE)
      )
    )
    val query3 = makeQuery(
      List(
        ColumnWithClause(Column("s1.t3.c1"), Clause.SELECT),
        ColumnWithClause(Column("s1.t3.c1"), Clause.GROUPBY),
        ColumnWithClause(Column("s1.t3.c2"), Clause.SELECT),
        ColumnWithClause(Column("s1.t3.c2"), Clause.WHERE)
      )
    )
    val clusters = Vector(List(query1), List(query2), List(query3))

    val matrix = Vector(
      Vector(3.0, 7.0, 2.0),
      Vector(4.0, 3.0, 8.0),
      Vector(0.0, 0.0, 0.0)
    ).map(row => row.map(_ / row.sum))

    val model = new MarkovModel(MarkovModelData(clusters, matrix, aligonMethod()), -1, -1)

    model.forecastQueries(query1.analysed) shouldBe List(query2)
    model.forecastQueries(query2.analysed) shouldBe List(query3)
    model.forecastQueries(query3.analysed) shouldBe List.empty
  }
}
