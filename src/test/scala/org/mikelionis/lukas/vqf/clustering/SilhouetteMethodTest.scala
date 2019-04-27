package org.mikelionis.lukas.vqf
package clustering

import TestsHelpers._
import query._

import org.scalatest.{FunSpec, Matchers}

class SilhouetteMethodTest extends FunSpec with Matchers {

  describe("clustering quality measurement") {
    it("should be 0.0 when there are no clusters") {
      silhouetteMethod().measureClusteringQuality(Vector.empty) shouldBe 0.0
    }

    it("should be 0.0 when there is only one cluster") {
      val columns1 = List(
        ColumnWithClause(Column("s1.t1.c1"), Clause.SELECT),
        ColumnWithClause(Column("s1.t1.c1"), Clause.GROUPBY),
        ColumnWithClause(Column("s1.t1.c2"), Clause.SELECT),
        ColumnWithClause(Column("s1.t1.c2"), Clause.WHERE)
      )
      val cluster1 = List(makeQuery(columns1))
      silhouetteMethod().measureClusteringQuality(Vector(cluster1)) shouldBe 0.0
    }

    it("should be 1.0 in best case scenario") {
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

      val cluster1 = List(makeQuery(columns1), makeQuery(columns1), makeQuery(columns1))
      val cluster2 = List(makeQuery(columns2), makeQuery(columns2), makeQuery(columns2))

      silhouetteMethod().measureClusteringQuality(Vector(cluster1, cluster2)) shouldBe 1.0
    }

    it("should be -1.0 / NUMBER_OF_CLUSTERS when queries are completely shuffled") {
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

      val cluster1 = List(makeQuery(columns1), makeQuery(columns2))
      val cluster2 = List(makeQuery(columns1), makeQuery(columns2))

      silhouetteMethod().measureClusteringQuality(Vector(cluster1, cluster2)) shouldBe (-1.0 / 2)
    }

    it("should be 0.0 when clusters are identical to each other") {
      val columns1 = List(
        ColumnWithClause(Column("s1.t1.c1"), Clause.SELECT),
        ColumnWithClause(Column("s1.t1.c1"), Clause.GROUPBY),
        ColumnWithClause(Column("s1.t1.c2"), Clause.SELECT),
        ColumnWithClause(Column("s1.t1.c2"), Clause.WHERE)
      )

      val cluster1 = List(makeQuery(columns1), makeQuery(columns1))
      val cluster2 = List(makeQuery(columns1), makeQuery(columns1))

      silhouetteMethod().measureClusteringQuality(Vector(cluster1, cluster2)) shouldBe 0.0
    }

    it("should be 0.0 when there are no similar queries") {
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
      val columns4 = List(
        ColumnWithClause(Column("s1.t4.c1"), Clause.SELECT),
        ColumnWithClause(Column("s1.t4.c1"), Clause.GROUPBY),
        ColumnWithClause(Column("s1.t4.c2"), Clause.SELECT),
        ColumnWithClause(Column("s1.t4.c2"), Clause.WHERE)
      )

      val cluster1 = List(makeQuery(columns1), makeQuery(columns2))
      val cluster2 = List(makeQuery(columns3), makeQuery(columns4))

      silhouetteMethod().measureClusteringQuality(Vector(cluster1, cluster2)) shouldBe 0.0
    }

    it("should be in (0;1) when only 1 out of 9 queries is misplaced") {
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
      val columns4 = List(
        ColumnWithClause(Column("s1.t4.c1"), Clause.SELECT),
        ColumnWithClause(Column("s1.t4.c1"), Clause.GROUPBY),
        ColumnWithClause(Column("s1.t4.c2"), Clause.SELECT),
        ColumnWithClause(Column("s1.t4.c2"), Clause.WHERE)
      )

      val cluster1 = List(makeQuery(columns1), makeQuery(columns1), makeQuery(columns1))
      val cluster2 = List(makeQuery(columns4), makeQuery(columns2), makeQuery(columns2))
      val cluster3 = List(makeQuery(columns3), makeQuery(columns3), makeQuery(columns3))

      val quality = silhouetteMethod().measureClusteringQuality(Vector(cluster1, cluster2, cluster3))
      quality should be > 0.0
      quality should be < 1.0
    }
  }

}
