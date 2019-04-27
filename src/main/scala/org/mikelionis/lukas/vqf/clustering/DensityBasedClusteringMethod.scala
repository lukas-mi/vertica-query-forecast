package org.mikelionis.lukas.vqf
package clustering

import query._
import similarity._

import scala.util.Random

class DensityBasedClusteringMethod(querySimilarityMethod: QuerySimilarityMethod, nOfSeeds: Int, maxClusterSize: Int)
    extends QueryClusteringMethod {
  require(nOfSeeds > 0, "nOfSeeds has to be greater than 0")
  require(maxClusterSize > 1, "maxClusterSize has to be greater than 1")

  private val queryClusterSimilarityMethod = new QueryClusterSimilarityMethod(querySimilarityMethod)

  private def splitCluster(cluster: List[Query]): Array[List[Query]] = {
    val queryPairs = for {
      query1 <- cluster
      query2 <- cluster
      if query1.metadata.epoch != query2.metadata.epoch
    } yield (query1, query2, querySimilarityMethod.measureSimilarity(query1.analysed, query2.analysed))

    val (seedQuery1, seedQuery2, _) = queryPairs.minBy { case (_, _, s) => s }
    val seedClusters = Array(List(seedQuery1), List(seedQuery2))

    def isSeedQuery(candidate: Query): Boolean =
      candidate.metadata.epoch == seedQuery1.metadata.epoch || candidate.metadata.epoch == seedQuery2.metadata.epoch

    cluster.foldLeft(seedClusters) {
      case (accClusters, candidate) =>
        if (isSeedQuery(candidate)) accClusters
        else assignQueryToCluster(accClusters, candidate)
    }
  }

  private def assignQueryToCluster(currentClusters: Array[List[Query]], candidate: Query): Array[List[Query]] = {
    val (maxAvgClusterSimilarity, clusterIndex) = currentClusters
      .map(queryClusterSimilarityMethod.measureSimilarity(candidate.analysed, _))
      .zipWithIndex
      .maxBy { case (avgClusterSimilarity, _) => avgClusterSimilarity }

    if (maxAvgClusterSimilarity != 0.0) {
      val assignedCluster = candidate +: currentClusters(clusterIndex)
      val otherClusters = currentClusters.take(clusterIndex) ++ currentClusters.drop(clusterIndex + 1)

      if (assignedCluster.size < maxClusterSize) assignedCluster +: otherClusters
      else splitCluster(assignedCluster) ++ otherClusters
    } else List(candidate) +: currentClusters
  }

  private def updateClusters(currentClusters: Array[List[Query]], queries: List[Query]): Array[List[Query]] =
    queries match {
      case head :: tail => updateClusters(assignQueryToCluster(currentClusters, head), tail)
      case Nil => currentClusters
    }

  override def clusterQueries(queries: List[Query]): Vector[List[Query]] = {
    val shuffledQueries = Random.shuffle(queries)
    val seedQueries = shuffledQueries.take(nOfSeeds)
    val seedClusters = seedQueries.map(List(_)).toArray
    val remainingQueries = shuffledQueries.drop(nOfSeeds)

    updateClusters(seedClusters, remainingQueries).toVector
  }
}
