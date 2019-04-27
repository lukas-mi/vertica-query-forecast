package org.mikelionis.lukas.vqf
package clustering

import query._
import similarity._

class QueryClusterSimilarityMethod(querySimilarityMethod: QuerySimilarityMethod) {
  private type MeasurementFun = (AnalysedQuery, AnalysedQuery) => Double

  private def measure(query: Query, cluster: List[Query])(fun: MeasurementFun): Double = {
    def loop(remainingCluster: List[Query], accMeasures: List[Double]): List[Double] =
      remainingCluster match {
        case clusterHead :: clusterTail =>
          if (clusterHead.metadata.epoch == query.metadata.epoch) loop(clusterTail, accMeasures)
          else loop(clusterTail, fun(query.analysed, clusterHead.analysed) :: accMeasures)
        case Nil => accMeasures
      }

    val measures = loop(cluster, List.empty)
    if (measures.isEmpty) 0.0 else measures.sum / measures.size
  }

  private def measure(query: AnalysedQuery, cluster: List[Query])(fun: MeasurementFun): Double =
    if (cluster.isEmpty) 0.0
    else cluster.map(clusterQuery => fun(query, clusterQuery.analysed)).sum / cluster.size

  def measureSimilarity(query: Query, cluster: List[Query]): Double =
    measure(query, cluster)(querySimilarityMethod.measureSimilarity)

  def measureDissimilarity(query: Query, cluster: List[Query]): Double =
    measure(query, cluster)(querySimilarityMethod.measureDissimilarity)

  def measureSimilarity(query: AnalysedQuery, cluster: List[Query]): Double =
    measure(query, cluster)(querySimilarityMethod.measureSimilarity)

  def measureDissimilarity(query: AnalysedQuery, cluster: List[Query]): Double =
    measure(query, cluster)(querySimilarityMethod.measureDissimilarity)
}
