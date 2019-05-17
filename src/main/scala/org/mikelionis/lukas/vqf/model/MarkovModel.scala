package org.mikelionis.lukas.vqf
package model

import clustering._
import query._

class MarkovModel(val data: MarkovModelData, probabilityCap: Double, similarityCap: Double) extends QueryForecastModel {
  private val method = new QueryClusterSimilarityMethod(data.querySimilarityMethod)

  override def forecastQueries(currentQuery: AnalysedQuery): List[Query] = {
    val clusterIndex = MarkovModelUtils.getMostSimilarClusterIndex(currentQuery, data.clusters, method)
    if (clusterIndex >= 0) {
      // if maxProbability is 0.0 then cluster is an absorbing state a.k.a always the last state
      val (maxProbability, nextClusterIndex) = data.probabilityMatrix(clusterIndex).zipWithIndex.maxBy(_._1)
      if (maxProbability > probabilityCap) {
        val nextCluster = data.clusters(nextClusterIndex)

        nextCluster
          .foldLeft(Option.empty[(Double, Query)]) {
            case (acc @ Some((maxSimilarity, _)), clusterQuery) =>
              val similarity = data.querySimilarityMethod.measureSimilarity(currentQuery, clusterQuery.analysed)
              if (similarity > maxSimilarity) Some((similarity, clusterQuery))
              else acc
            case (None, clusterQuery) =>
              val similarity = data.querySimilarityMethod.measureSimilarity(currentQuery, clusterQuery.analysed)
              Some((similarity, clusterQuery))
          }
          .collect {
            case (maxSimilarity, clusterQuery) if maxSimilarity > probabilityCap => clusterQuery
          }
          .toList

      } else List.empty
    } else List.empty
  }
}
