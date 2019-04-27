package org.mikelionis.lukas.vqf
package model

import query._
import similarity._
import clustering._
import serialization.JsonProtocol._
import fs._

import java.io.File

class MarkovModelFactory(
    querySimilarityMethod: QuerySimilarityMethod,
    queryClusteringMethod: QueryClusteringMethod,
    topNQueries: Int
) extends QueryForecastModelFactory[MarkovModel] {

  private val queryClusterSimilarityMethod = new QueryClusterSimilarityMethod(querySimilarityMethod)

  override def createModel(queries: List[Query]): MarkovModel = {
    val groupedQueries = MarkovModelUtils.groupQueries(queries)
    val queriesToCluster = groupedQueries.values.flatten.toList
    val clusters = queryClusteringMethod.clusterQueries(queriesToCluster)

    val sessionsIndexes = groupedQueries.mapValues { sessionQueries =>
      MarkovModelUtils.queriesToClusterIndexPairs(sessionQueries, clusters, queryClusterSimilarityMethod)
    }

    val matrix = MarkovModelUtils.createProbabilityMatrix(sessionsIndexes.values.flatten.toList, clusters.size)

    new MarkovModel(MarkovModelData(clusters, matrix, querySimilarityMethod))
  }

  override def saveModel(file: File, model: MarkovModel): Unit = FileUtils.writeJSON(file, model.data)

  override def loadModel(file: File): MarkovModel = new MarkovModel(FileUtils.readJSON[MarkovModelData](file))
}
