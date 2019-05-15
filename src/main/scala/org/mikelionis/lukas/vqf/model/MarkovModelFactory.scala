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
    topNQueries: Int,
    probabilityCap: Double = 0.0,
    similarityCap: Double = 0.0
) extends QueryForecastModelFactory[MarkovModel] {

  private val queryClusterSimilarityMethod = new QueryClusterSimilarityMethod(querySimilarityMethod)

  override def createModel(queries: List[Query]): MarkovModel = {
    val groupedQueries = MarkovModelUtils.groupQueries(queries)
    val clusters = queryClusteringMethod.clusterQueries(groupedQueries.flatMap(_._2))
    val clusterIndexes = groupedQueries.flatMap {
      case (_, sessionQueries) =>
        MarkovModelUtils.queriesToClusterIndexPairs(sessionQueries, clusters, queryClusterSimilarityMethod)
    }
    val matrix = MarkovModelUtils.createProbabilityMatrix(clusterIndexes, clusters.size)
    new MarkovModel(MarkovModelData(clusters, matrix, querySimilarityMethod), probabilityCap, similarityCap)
  }

  override def saveModel(file: File, model: MarkovModel): Unit = FileUtils.writeJSON(file, model.data)

  override def loadModel(file: File): MarkovModel =
    new MarkovModel(FileUtils.readJSON[MarkovModelData](file), probabilityCap, similarityCap)
}
