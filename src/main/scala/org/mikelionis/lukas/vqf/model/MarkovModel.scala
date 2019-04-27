package org.mikelionis.lukas.vqf
package model

import clustering._
import query._

class MarkovModel(val data: MarkovModelData) extends QueryForecastModel {
  private val method = new QueryClusterSimilarityMethod(data.querySimilarityMethod)

  override def forecastQueries(currentQuery: AnalysedQuery): List[Query] = {
    val clusterIndex = MarkovModelUtils.getMostSimilarClusterIndex(currentQuery, data.clusters, method)
    if (clusterIndex >= 0) {
      // if maxProbability is 0.0 then cluster is an absorbing state a.k.a always the last state
      val (maxProbability, nextClusterIndex) = data.probabilityMatrix(clusterIndex).zipWithIndex.maxBy(_._1)
      if (maxProbability > 0.0) {
        val nextCluster = data.clusters(nextClusterIndex)
        val nextQuery = nextCluster.maxBy(q => data.querySimilarityMethod.measureSimilarity(currentQuery, q.analysed))
        List(nextQuery)
      } else List.empty
    } else List.empty
  }
}
