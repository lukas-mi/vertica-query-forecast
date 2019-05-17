package org.mikelionis.lukas.vqf
package model

import java.time.ZoneOffset

import query._
import clustering._

object MarkovModelUtils {
  private def getMostSimilarClusterIndexI(
      query: AnalysedQuery,
      indexedClusters: Vector[(List[Query], Int)],
      method: QueryClusterSimilarityMethod
  ): Int = {
    val (index, _) = indexedClusters.foldLeft((-1, 0.0)) {
      case ((mostSimilarClusterIndex, maxSimilarity), (cluster, clusterIndex)) =>
        val similarity = method.measureSimilarity(query, cluster)
        if (similarity > maxSimilarity) clusterIndex -> similarity
        else mostSimilarClusterIndex -> maxSimilarity
    }
    index
  }

  def getMostSimilarClusterIndex(
      query: AnalysedQuery,
      clusters: Vector[List[Query]],
      method: QueryClusterSimilarityMethod
  ): Int = getMostSimilarClusterIndexI(query, clusters.zipWithIndex, method)

  private def queriesToClusterIndexPairs(
      queries: List[Query],
      indexedClusters: Vector[(List[Query], Int)],
      method: QueryClusterSimilarityMethod,
      accIndexes: List[(Int, Int)]
  ): List[(Int, Int)] =
    queries match {
      case query1 :: (tail @ query2 :: _) =>
        val cluster1Index = getMostSimilarClusterIndexI(query1.analysed, indexedClusters, method)
        val cluster2Index = getMostSimilarClusterIndexI(query2.analysed, indexedClusters, method)
        queriesToClusterIndexPairs(tail, indexedClusters, method, (cluster1Index -> cluster2Index) :: accIndexes)
      case _ => accIndexes
    }

  def queriesToClusterIndexPairs(
      queries: List[Query],
      clusters: Vector[List[Query]],
      method: QueryClusterSimilarityMethod
  ): List[(Int, Int)] =
    if (clusters.isEmpty) List.empty
    else queriesToClusterIndexPairs(queries, clusters.zipWithIndex, method, List.empty).reverse

  private def removeDuplicates(queries: List[Query], accQueries: List[Query]): List[Query] =
    (queries, accQueries) match {
      case (queriesHead :: queriesTail, Nil) => removeDuplicates(queriesTail, List(queriesHead))
      case (queriesHead :: queriesTail, accQueriesHead :: _) =>
        if (queriesHead.analysed.columns == accQueriesHead.analysed.columns) removeDuplicates(queriesTail, accQueries)
        else removeDuplicates(queriesTail, queriesHead :: accQueries)
      case (Nil, _) => accQueries
    }

  // 1. remove queries without columns (SELECT COUNT(*) FROM foo.bar)
  // 2. group queries by session
  // 3. remove consecutive duplicate queries from each sessions
  // 4. remove sessions with only one query
  // 5. sort sessions by their first query start time
  def groupQueries(queries: List[Query]): List[(String, List[Query])] =
    queries
      .filter(_.analysed.columns.nonEmpty)
      .groupBy(_.metadata.session)
      .mapValues(sessionQueries => removeDuplicates(sessionQueries.sortBy(_.metadata.epoch), List.empty).reverse)
      .filter { case (_, sessionQueries) => sessionQueries.size > 1 }
      .toList
      .sortBy {
        case (_, sessionQueries) =>
          def accFun(minStart: Long, currentQuery: Query): Long = {
            val currentStart = currentQuery.metadata.start.toEpochSecond(ZoneOffset.UTC)
            if (currentStart < minStart) currentStart else minStart
          }
          sessionQueries.foldLeft(Long.MaxValue)(accFun)
      }

  def createProbabilityMatrix(indexes: List[(Int, Int)], dimension: Int): Vector[Vector[Double]] = {
    val matrix = Array.fill(dimension)(Array.fill(dimension)(0.0))
    indexes
      .groupBy { case (fromIndex, _) => fromIndex }
      .foreach {
        case (fromIndex, indexPairs) =>
          indexPairs.foreach {
            case (_, toIndex) =>
              val oldValue = matrix(fromIndex)(toIndex)
              matrix(fromIndex)(toIndex) = oldValue + 1
          }
          matrix(fromIndex).transform(_ / indexPairs.size)
      }
    matrix.toVector.map(_.toVector)
  }
}
