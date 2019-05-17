package org.mikelionis.lukas.vqf
package clustering

import query._
import similarity._

class DbscanMinNoiseMethod(
    eps: Double,
    epsGrowthFactor: Double,
    minNeighborsSize: Int,
    withNoise: Boolean,
    similarityMethod: QuerySimilarityMethod
) extends QueryClusteringMethod {
  require(eps >= 0.0 && eps <= 1.0, "eps has to be in [0.0;1.0] range")
  require(epsGrowthFactor > 1.0, "epsGrowthFactor has to be greater than 1.0")
  require(minNeighborsSize >= 1, "minNeighborsSize has to be greater than 1")

  private def runDbscan(newEps: Double, queries: List[Query]): Vector[List[Query]] =
    new DbscanMethod(newEps, minNeighborsSize, true, similarityMethod).clusterQueries(queries)

  override def clusterQueries(queries: List[Query]): Vector[List[Query]] = {
    def loop(currentEps: Double, noiseCluster: List[Query], accClusters: Vector[List[Query]]): Vector[List[Query]] =
      if (currentEps < 1.0 && noiseCluster.size > 1) {
        val newClusters = runDbscan(currentEps, noiseCluster)

        val newNoiseCluster = newClusters.head
        val newClustersWithoutNoise = newClusters.tail
        val newEps = currentEps * epsGrowthFactor

        loop(newEps, newNoiseCluster, newClustersWithoutNoise ++ accClusters)
      } else {
        if (withNoise) noiseCluster +: accClusters else accClusters
      }

    if (queries.nonEmpty) {
      val newClusters = runDbscan(eps, queries)
      if (eps == 0.0 || eps == 1.0) newClusters
      else loop(eps * epsGrowthFactor, newClusters.head, newClusters.tail)
    } else Vector.empty
  }
}
