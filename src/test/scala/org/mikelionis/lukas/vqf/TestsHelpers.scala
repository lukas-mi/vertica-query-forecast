package org.mikelionis.lukas.vqf

import query._
import similarity._
import clustering._
import model._

import java.io.File
import java.nio.file.Files
import java.time.{Duration, LocalDateTime, ZoneOffset}
import org.scalatest.Matchers

import scala.util.Random

object TestsHelpers extends Matchers {
  def randomPrintableString(size: Int = 12): String = Seq.fill(12)(Random.nextPrintableChar).mkString

  def aligonMethod(weights: Weights = Weights.default): AligonMethod =
    new AligonMethod(weights)

  def queryClusterSimilarityMethod(innerMethod: QuerySimilarityMethod = aligonMethod()): QueryClusterSimilarityMethod =
    new QueryClusterSimilarityMethod(innerMethod)

  def silhouetteMethod(innerMethod: QuerySimilarityMethod = aligonMethod()): SilhouetteMethod =
    new SilhouetteMethod(innerMethod)

  def markovModelFactory(
      querySimilarityMethod: QuerySimilarityMethod = aligonMethod(),
      queryClusteringMethod: QueryClusteringMethod = new DensityBasedClusteringMethod(aligonMethod(), 5, 2),
      topNQueries: Int = 1
  ): MarkovModelFactory = new MarkovModelFactory(querySimilarityMethod, queryClusteringMethod, topNQueries)

  def makeQuery(columns: ColumnWithClause*): AnalysedQuery =
    AnalysedQuery(Statement.SELECT, Set.empty, Set.empty, columns.toSet, Set.empty)

  def makeQuery(
      columns: List[ColumnWithClause],
      file: File = Files.createTempFile("query", "").toFile,
      user: String = randomPrintableString(),
      epoch: Int = Random.nextInt,
      session: String = randomPrintableString(),
      start: LocalDateTime = LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC),
      duration: Duration = Duration.ofMillis(Random.nextLong())
  ): Query =
    Query(
      file,
      AnalysedQuery(Statement.SELECT, Set.empty, Set.empty, columns.toSet, Set.empty),
      QueryMetadata(user, epoch, session, start, duration)
    )

  // calculate similarity and ensure that order does not matter, also ensure range [0;1]
  def measureSimilarity(query1: AnalysedQuery, query2: AnalysedQuery, method: QuerySimilarityMethod): Double = {
    val sim1 = method.measureSimilarity(query1, query2)
    val sim2 = method.measureSimilarity(query2, query1)

    sim1 shouldBe sim2
    sim1 should be >= 0.0
    sim1 should be <= 1.0
    sim1
  }

  // calculate dissimilarity and ensure that order does not matter, also ensure range [0;1]
  def measureDissimilarity(query1: AnalysedQuery, query2: AnalysedQuery, method: QuerySimilarityMethod): Double = {
    val dis1 = method.measureDissimilarity(query1, query2)
    val dis2 = method.measureDissimilarity(query2, query1)

    dis1 shouldBe dis2
    dis1 should be >= 0.0
    dis1 should be <= 1.0
    dis1
  }

  // calculate similarity and ensure range [0;1]
  def measureSimilarity(
      query: Query,
      cluster: List[Query],
      method: QueryClusterSimilarityMethod
  ): Double = {
    val sim = method.measureSimilarity(query, cluster)

    sim should be >= 0.0
    sim should be <= 1.0
    sim
  }

  // calculate dissimilarity and ensure range [0;1]
  def measureDissimilarity(
      query: Query,
      cluster: List[Query],
      method: QueryClusterSimilarityMethod
  ): Double = {
    val sim = method.measureDissimilarity(query, cluster)

    sim should be >= 0.0
    sim should be <= 1.0
    sim
  }

  // calculate similarity and ensure range [0;1]
  def measureSimilarity(
      query: AnalysedQuery,
      cluster: List[Query],
      method: QueryClusterSimilarityMethod
  ): Double = {
    val sim = method.measureSimilarity(query, cluster)

    sim should be >= 0.0
    sim should be <= 1.0
    sim
  }

  // calculate dissimilarity and ensure range [0;1]
  def measureDissimilarity(
      query: AnalysedQuery,
      cluster: List[Query],
      method: QueryClusterSimilarityMethod
  ): Double = {
    val sim = method.measureDissimilarity(query, cluster)

    sim should be >= 0.0
    sim should be <= 1.0
    sim
  }

  def measureBoth(query1: AnalysedQuery, query2: AnalysedQuery, method: QuerySimilarityMethod): (Double, Double) =
    (measureSimilarity(query1, query2, method), measureDissimilarity(query1, query2, method))

  def measureBoth(
      query: Query,
      cluster: List[Query],
      method: QueryClusterSimilarityMethod,
  ): (Double, Double) = (measureSimilarity(query, cluster, method), measureDissimilarity(query, cluster, method))

  def measureBoth(
      query: AnalysedQuery,
      cluster: List[Query],
      method: QueryClusterSimilarityMethod,
  ): (Double, Double) = (measureSimilarity(query, cluster, method), measureDissimilarity(query, cluster, method))

}
