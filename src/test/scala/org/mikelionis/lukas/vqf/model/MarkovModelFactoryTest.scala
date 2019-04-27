package org.mikelionis.lukas.vqf
package model

import TestsHelpers._
import query._
import similarity._

import org.scalatest.{FunSpec, Matchers}

import java.nio.file.Files
import scala.util.Random

class MarkovModelFactoryTest extends FunSpec with Matchers {
  it("should save and load model") {
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

    val querySimilarityMethod = aligonMethod()
    val model = new MarkovModel(MarkovModelData(clusters, matrix, querySimilarityMethod))

    val factory = markovModelFactory()
    val modelFile = Files.createTempFile("markov_model", "").toFile

    factory.saveModel(modelFile, model)
    val loadedModel = factory.loadModel(modelFile)

    loadedModel.data.clusters should contain theSameElementsInOrderAs model.data.clusters
    loadedModel.data.probabilityMatrix should contain theSameElementsInOrderAs model.data.probabilityMatrix
    loadedModel.data.querySimilarityMethod shouldBe a[AligonMethod]
    loadedModel.data.querySimilarityMethod.asInstanceOf[AligonMethod].weights shouldBe querySimilarityMethod.weights
  }
}
