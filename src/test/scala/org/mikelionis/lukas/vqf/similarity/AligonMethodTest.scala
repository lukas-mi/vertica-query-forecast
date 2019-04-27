package org.mikelionis.lukas.vqf
package similarity

import TestsHelpers._
import query._

import org.scalatest.{FunSpec, Matchers}

class AligonMethodTest extends FunSpec with Matchers {

  it("similarity and dissimilarity should be 1.0 and 0.0 respectively for identical queries") {
    val query1 = makeQuery(
      ColumnWithClause(Column("s1.t1.c1"), Clause.SELECT),
      ColumnWithClause(Column("s1.t1.c1"), Clause.GROUPBY),
      ColumnWithClause(Column("s1.t1.c2"), Clause.SELECT),
      ColumnWithClause(Column("s1.t1.c2"), Clause.WHERE),
      ColumnWithClause(Column("s1.t1.c2"), Clause.HAVING)
    )

    measureSimilarity(query1, query1, aligonMethod()) shouldBe 1.0
    measureDissimilarity(query1, query1, aligonMethod()) shouldBe 0.0
  }

  it("similarity and dissimilarity should be 0.0 and 1.0 respectively for completely different queries") {
    val query1 = makeQuery(
      ColumnWithClause(Column("s1.t1.c1"), Clause.SELECT),
      ColumnWithClause(Column("s1.t1.c1"), Clause.GROUPBY),
      ColumnWithClause(Column("s1.t1.c2"), Clause.SELECT),
      ColumnWithClause(Column("s1.t1.c2"), Clause.WHERE),
      ColumnWithClause(Column("s1.t1.c2"), Clause.HAVING)
    )
    val query2 = makeQuery(
      ColumnWithClause(Column("s1.t2.c1"), Clause.SELECT),
      ColumnWithClause(Column("s1.t2.c1"), Clause.GROUPBY),
      ColumnWithClause(Column("s1.t2.c2"), Clause.SELECT),
      ColumnWithClause(Column("s1.t2.c2"), Clause.WHERE),
      ColumnWithClause(Column("s1.t2.c2"), Clause.HAVING)
    )

    measureSimilarity(query1, query2, aligonMethod()) shouldBe 0.0
    measureDissimilarity(query1, query2, aligonMethod()) shouldBe 1.0
  }

  // SELECT COUNT(*) FROM foo.bar;
  it("similarity and dissimilarity should both be 0.0 when both sets are empty") {
    val query1 = makeQuery()
    val query2 = makeQuery()

    measureSimilarity(query1, query2, aligonMethod()) shouldBe 0.0
    measureDissimilarity(query1, query2, aligonMethod()) shouldBe 0.0
  }

  describe("similarity and dissimilarity when queries are somewhat similar/dissimilar") {
    // query1 has additional column in HAVING clause, however it also appears in its WHERE clause
    it("similarity and dissimilarity should be 1.0 and 0.0 respectively when columns sets are identical") {
      val query1 = makeQuery(
        ColumnWithClause(Column("s1.t1.c1"), Clause.SELECT),
        ColumnWithClause(Column("s1.t1.c1"), Clause.GROUPBY),
        ColumnWithClause(Column("s1.t1.c2"), Clause.SELECT),
        ColumnWithClause(Column("s1.t1.c2"), Clause.WHERE),
        ColumnWithClause(Column("s1.t1.c2"), Clause.HAVING)
      )
      val query2 = makeQuery(
        ColumnWithClause(Column("s1.t1.c1"), Clause.SELECT),
        ColumnWithClause(Column("s1.t1.c1"), Clause.GROUPBY),
        ColumnWithClause(Column("s1.t1.c2"), Clause.SELECT),
        ColumnWithClause(Column("s1.t1.c2"), Clause.WHERE)
      )

      measureSimilarity(query1, query2, aligonMethod()) shouldBe 1.0
      measureDissimilarity(query2, query1, aligonMethod()) shouldBe 0.0
    }

    it("similarity should be higher than dissimilarity when queries differ by 1 out of 4 columns") {
      val query1 = makeQuery(
        ColumnWithClause(Column("s1.t1.c1"), Clause.SELECT),
        ColumnWithClause(Column("s1.t1.c1"), Clause.GROUPBY),
        ColumnWithClause(Column("s1.t1.c2"), Clause.SELECT),
        ColumnWithClause(Column("s1.t1.c2"), Clause.WHERE)
      )
      val query2 = makeQuery(
        ColumnWithClause(Column("s1.t1.c1"), Clause.SELECT),
        ColumnWithClause(Column("s1.t1.c1"), Clause.GROUPBY),
        ColumnWithClause(Column("s1.t1.c2"), Clause.SELECT)
      )

      measureSimilarity(query1, query2, aligonMethod()) should be > measureDissimilarity(query2, query1, aligonMethod())
    }

    it("similarity should be lower than dissimilarity when queries differ by 3 out of 4 columns") {
      val query1 = makeQuery(
        ColumnWithClause(Column("s1.t1.c1"), Clause.SELECT),
        ColumnWithClause(Column("s1.t1.c1"), Clause.GROUPBY),
        ColumnWithClause(Column("s1.t1.c2"), Clause.SELECT),
        ColumnWithClause(Column("s1.t1.c2"), Clause.WHERE),
        ColumnWithClause(Column("s1.t1.c3"), Clause.GROUPBY),
        ColumnWithClause(Column("s1.t1.c3"), Clause.WHERE)
      )
      val query2 = makeQuery(
        ColumnWithClause(Column("s1.t1.c1"), Clause.SELECT),
        ColumnWithClause(Column("s1.t1.c1"), Clause.GROUPBY),
        ColumnWithClause(Column("s1.t1.c2"), Clause.SELECT)
      )

      measureSimilarity(query1, query2, aligonMethod()) should be < measureDissimilarity(query1, query2, aligonMethod())
    }
  }

  describe("effects of 0.0 coefficient on query similarity/dissimilarity") {
    it("difference in 'projection' columns should not matter when 'projection' coefficient is 0.0") {
      val weights = Weights(0.0, 0.5, 0.5)
      val query1 = makeQuery(
        ColumnWithClause(Column("s1.t1.c1"), Clause.SELECT),
        ColumnWithClause(Column("s1.t1.c1"), Clause.GROUPBY),
        ColumnWithClause(Column("s1.t1.c2"), Clause.SELECT),
        ColumnWithClause(Column("s1.t1.c2"), Clause.WHERE)
      )
      val query2 = makeQuery(
        ColumnWithClause(Column("s1.t1.c1"), Clause.SELECT),
        ColumnWithClause(Column("s1.t1.c1"), Clause.GROUPBY),
        ColumnWithClause(Column("s1.t1.c2"), Clause.WHERE)
      )

      measureSimilarity(query1, query2, aligonMethod(weights)) shouldBe 1.0
      measureDissimilarity(query1, query2, aligonMethod(weights)) shouldBe 0.0
    }

    it("difference in 'selection' columns should not matter when 'selection' coefficient is 0.0") {
      val weights = Weights(0.5, 0.0, 0.5)
      val query1 = makeQuery(
        ColumnWithClause(Column("s1.t1.c1"), Clause.SELECT),
        ColumnWithClause(Column("s1.t1.c1"), Clause.GROUPBY),
        ColumnWithClause(Column("s1.t1.c2"), Clause.SELECT),
        ColumnWithClause(Column("s1.t1.c2"), Clause.WHERE)
      )
      val query2 = makeQuery(
        ColumnWithClause(Column("s1.t1.c1"), Clause.SELECT),
        ColumnWithClause(Column("s1.t1.c1"), Clause.GROUPBY),
        ColumnWithClause(Column("s1.t1.c2"), Clause.SELECT),
      )

      measureSimilarity(query1, query2, aligonMethod(weights)) shouldBe 1.0
      measureDissimilarity(query1, query2, aligonMethod(weights)) shouldBe 0.0
    }

    it("difference in 'group by' columns should not matter when 'group by' coefficient is 0.0") {
      val weights = Weights(0.5, 0.5, 0.0)
      val query1 = makeQuery(
        ColumnWithClause(Column("s1.t1.c1"), Clause.SELECT),
        ColumnWithClause(Column("s1.t1.c1"), Clause.GROUPBY),
        ColumnWithClause(Column("s1.t1.c2"), Clause.SELECT),
        ColumnWithClause(Column("s1.t1.c2"), Clause.WHERE)
      )
      val query2 = makeQuery(
        ColumnWithClause(Column("s1.t1.c1"), Clause.SELECT),
        ColumnWithClause(Column("s1.t1.c2"), Clause.SELECT),
        ColumnWithClause(Column("s1.t1.c2"), Clause.WHERE)
      )

      measureSimilarity(query1, query2, aligonMethod(weights)) shouldBe 1.0
      measureDissimilarity(query1, query2, aligonMethod(weights)) shouldBe 0.0
    }
  }

  describe("effects of different coefficient ratios on query similarity/dissimilarity") {
    it("difference in 'projection' coefficient should effect query similarity/dissimilarity differently") {
      val query1 = makeQuery(
        ColumnWithClause(Column("s1.t1.c1"), Clause.SELECT),
        ColumnWithClause(Column("s1.t1.c1"), Clause.GROUPBY),
        ColumnWithClause(Column("s1.t1.c2"), Clause.SELECT),
        ColumnWithClause(Column("s1.t1.c2"), Clause.WHERE)
      )
      val query2 = makeQuery(
        ColumnWithClause(Column("s1.t1.c1"), Clause.SELECT),
        ColumnWithClause(Column("s1.t1.c1"), Clause.GROUPBY),
        ColumnWithClause(Column("s1.t1.c2"), Clause.WHERE)
      )

      val weights1 = Weights(0.4, 0.3, 0.3)
      val weights2 = Weights(0.6, 0.2, 0.2)

      val (sim1, dis1) = measureBoth(query1, query2, aligonMethod(weights1))
      val (sim2, dis2) = measureBoth(query1, query2, aligonMethod(weights2))

      sim1 should be > sim2
      dis1 should be < dis2
    }

    it("difference in 'selection' coefficient should effect query similarity/dissimilarity differently") {
      val query1 = makeQuery(
        ColumnWithClause(Column("s1.t1.c1"), Clause.SELECT),
        ColumnWithClause(Column("s1.t1.c1"), Clause.GROUPBY),
        ColumnWithClause(Column("s1.t1.c2"), Clause.SELECT),
        ColumnWithClause(Column("s1.t1.c2"), Clause.WHERE)
      )
      val query2 = makeQuery(
        ColumnWithClause(Column("s1.t1.c1"), Clause.SELECT),
        ColumnWithClause(Column("s1.t1.c1"), Clause.GROUPBY),
        ColumnWithClause(Column("s1.t1.c2"), Clause.SELECT)
      )

      val weights1 = Weights(0.3, 0.4, 0.3)
      val weights2 = Weights(0.2, 0.6, 0.2)

      val (sim1, dis1) = measureBoth(query1, query2, aligonMethod(weights1))
      val (sim2, dis2) = measureBoth(query1, query2, aligonMethod(weights2))

      sim1 should be > sim2
      dis1 should be < dis2
    }

    it("difference in 'group by' coefficient should effect query similarity/dissimilarity differently") {
      val query1 = makeQuery(
        ColumnWithClause(Column("s1.t1.c1"), Clause.SELECT),
        ColumnWithClause(Column("s1.t1.c1"), Clause.GROUPBY),
        ColumnWithClause(Column("s1.t1.c2"), Clause.SELECT),
        ColumnWithClause(Column("s1.t1.c2"), Clause.WHERE)
      )
      val query2 = makeQuery(
        ColumnWithClause(Column("s1.t1.c1"), Clause.SELECT),
        ColumnWithClause(Column("s1.t1.c2"), Clause.SELECT),
        ColumnWithClause(Column("s1.t1.c2"), Clause.WHERE)
      )

      val weights1 = Weights(0.3, 0.3, 0.4)
      val weights2 = Weights(0.2, 0.2, 0.6)

      val (sim1, dis1) = measureBoth(query1, query2, aligonMethod(weights1))
      val (sim2, dis2) = measureBoth(query1, query2, aligonMethod(weights2))

      sim1 should be > sim2
      dis1 should be < dis2
    }
  }

}
