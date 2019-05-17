package org.mikelionis.lukas.vqf
package clustering

import query._
import similarity._

class SilhouetteMethod(querySimilarityMethod: QuerySimilarityMethod) extends QueryClusteringQualityMethod {
  private val queryClusterSimilarityMethod = new QueryClusterSimilarityMethod(querySimilarityMethod)

  private def calcSilhouette(intraClusterDissimilarity: Double, interClusterDissimilarityMin: Double): Double =
    if (intraClusterDissimilarity < interClusterDissimilarityMin)
      1.0 - intraClusterDissimilarity / interClusterDissimilarityMin
    else if (intraClusterDissimilarity > interClusterDissimilarityMin)
      interClusterDissimilarityMin / intraClusterDissimilarity - 1.0
    else 0.0

  override def measureClusteringQuality(clusters: Vector[List[Query]]): Double =
    if (clusters.size > 1) {
      val indexedClusters = clusters.zipWithIndex
      val silhouetteCoefficients = for {
        (cluster, index) <- indexedClusters
        query <- cluster
      } yield
        if (cluster.size > 1) {
          val intraClusterDissimilarity = queryClusterSimilarityMethod.measureDissimilarity(query, cluster)
          val interClusterDissimilarityMin = indexedClusters.foldLeft(Double.MaxValue) {
            case (minAvg, (otherCluster, otherClusterIndex)) =>
              if (otherClusterIndex != index) {
                val avg = queryClusterSimilarityMethod.measureDissimilarity(query, otherCluster)
                if (avg < minAvg) avg else minAvg
              } else minAvg
          }

          calcSilhouette(intraClusterDissimilarity, interClusterDissimilarityMin)
        } else 0.0

      silhouetteCoefficients.sum / silhouetteCoefficients.size
    } else 0.0
}
