package org.mikelionis.lukas.vqf
package model

import query._
import TestsHelpers._

import org.scalatest.{FunSpec, Matchers}

import scala.util.Random

class MarkovModelUtilsTest extends FunSpec with Matchers {
  it("groupQueries should remove queries with no columns, consecutive query duplicates in sessions and empty sessions") {
    val session1 = "session-1"
    val session2 = "session-2"
    val session3 = "session-3"

    val columns1 = List(
      ColumnWithClause(Column("s1.t1.c1"), Clause.SELECT),
      ColumnWithClause(Column("s1.t1.c1"), Clause.GROUPBY),
      ColumnWithClause(Column("s1.t1.c2"), Clause.SELECT),
      ColumnWithClause(Column("s1.t1.c2"), Clause.WHERE)
    )

    val columns2 = List(
      ColumnWithClause(Column("s1.t2.c1"), Clause.SELECT),
      ColumnWithClause(Column("s1.t2.c1"), Clause.GROUPBY),
      ColumnWithClause(Column("s1.t2.c2"), Clause.SELECT),
      ColumnWithClause(Column("s1.t2.c2"), Clause.WHERE)
    )

    val columns3 = List(
      ColumnWithClause(Column("s1.t1.c1"), Clause.SELECT),
      ColumnWithClause(Column("s1.t1.c1"), Clause.GROUPBY),
      ColumnWithClause(Column("s1.t1.c2"), Clause.SELECT),
      ColumnWithClause(Column("s1.t1.c2"), Clause.WHERE)
    )

    val s1q1 = makeQuery(columns1, session = session1, epoch = 11)
    val s1q2 = makeQuery(columns2, session = session1, epoch = 12)
    val s1q3 = makeQuery(columns2, session = session1, epoch = 13)
    val s1q4 = makeQuery(columns3, session = session1, epoch = 14)

    val s2q1 = makeQuery(columns1, session = session2, epoch = 21)
    val s2q2 = makeQuery(columns2, session = session2, epoch = 22)
    val s2q3 = makeQuery(columns3, session = session2, epoch = 23)
    val s2q4 = makeQuery(columns3, session = session2, epoch = 24)
    val s2q5 = makeQuery(List.empty, session = session2, epoch = 25)

    val s3q1 = makeQuery(columns1, session = session3, epoch = 31)
    val s3q2 = makeQuery(columns1, session = session3, epoch = 32)
    val s3q3 = makeQuery(columns1, session = session3, epoch = 33)

    val session1Queries = List(s1q1, s1q2, s1q3, s1q4)
    val session2Queries = List(s2q1, s2q2, s2q3, s2q4, s2q5)
    val session3Queries = List(s3q1, s3q2, s3q3)

    val preparedQueries =
      MarkovModelUtils.groupQueries(Random.shuffle(session1Queries ::: session2Queries ::: session3Queries))
    val expectedQueries = Map(
      session1 -> List(s1q1, s1q2, s1q4),
      session2 -> List(s2q1, s2q2, s2q3)
    )

    preparedQueries should contain theSameElementsAs expectedQueries
  }

  it("queriesToClusterIndexPairs should map consecutive query pairs to cluster index pairs") {
    val columns1 = List(
      ColumnWithClause(Column("s1.t1.c1"), Clause.SELECT),
      ColumnWithClause(Column("s1.t1.c1"), Clause.GROUPBY),
      ColumnWithClause(Column("s1.t1.c2"), Clause.SELECT),
      ColumnWithClause(Column("s1.t1.c2"), Clause.WHERE)
    )
    val columns2 = List(
      ColumnWithClause(Column("s1.t2.c1"), Clause.SELECT),
      ColumnWithClause(Column("s1.t2.c1"), Clause.GROUPBY),
      ColumnWithClause(Column("s1.t2.c2"), Clause.SELECT),
      ColumnWithClause(Column("s1.t2.c2"), Clause.WHERE)
    )
    val columns3 = List(
      ColumnWithClause(Column("s1.t3.c1"), Clause.SELECT),
      ColumnWithClause(Column("s1.t3.c1"), Clause.GROUPBY),
      ColumnWithClause(Column("s1.t3.c2"), Clause.SELECT),
      ColumnWithClause(Column("s1.t3.c2"), Clause.WHERE)
    )

    val query1 = makeQuery(columns1)
    val query2 = makeQuery(columns2)
    val query3 = makeQuery(columns3)
    val queries = List(query1, query2, query1, query1, query3, query2)

    val cluster1 = List(query1)
    val cluster2 = List(query2)
    val clusters = Vector(cluster1, cluster2)

    // -1 when query does not belong to any cluster
    val expectedIndexes = List((0, 1), (1, 0), (0, 0), (0, -1), (-1, 1))
    val indexes = MarkovModelUtils.queriesToClusterIndexPairs(queries, clusters, queryClusterSimilarityMethod())

    indexes should contain theSameElementsInOrderAs expectedIndexes
  }

  it("createProbabilityMatrix should create matrix based upon index pairs") {
    val dimension = 4
    val indexes = List(
      (0, 1),
      (0, 1),
      (0, 0),
      (0, 2),
      (1, 0),
      (1, 0),
      (1, 1),
      (1, 2),
      (1, 3),
      (2, 1),
      (2, 2),
      (2, 2),
      (2, 3)
    )

    val expectedMatrix = Vector(
      Vector(1.0, 2.0, 1.0, 0.0).map(_ / 4),
      Vector(2.0, 1.0, 1.0, 1.0).map(_ / 5),
      Vector(0.0, 1.0, 2.0, 1.0).map(_ / 4),
      Vector(0.0, 0.0, 0.0, 0.0) // #3 is an absorbing state
    )
    val matrix = MarkovModelUtils.createProbabilityMatrix(Random.shuffle(indexes), dimension)

    matrix should contain theSameElementsInOrderAs expectedMatrix
  }
}
