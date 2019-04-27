package org.mikelionis.lukas.vqf
package similarity

import query._

class AligonMethod(val weights: Weights) extends QuerySimilarityMethod {
  private def calcJaccardSimilarity(columns1: Set[Column], columns2: Set[Column]): Option[Double] = {
    val intersection = columns1 & columns2
    val union = columns1 | columns2

    if (union.nonEmpty) {
      val intersectionSize = intersection.size.toDouble
      val unionSize = union.size.toDouble
      Some(intersectionSize / unionSize)
    } else None
  }

  private def calcJaccardDissimilarity(columns1: Set[Column], columns2: Set[Column]): Option[Double] =
    calcJaccardSimilarity(columns1, columns2).map(1.0 - _)

  private def withWeight(measurementOpt: Option[Double], weight: Double): (Double, Double) =
    measurementOpt.fold(0.0 -> 0.0)(_ -> weight)

  private def calculate(f1: Features, f2: Features)(fun: (Set[Column], Set[Column]) => Option[Double]): Double = {
    val (projection, projectionW) = withWeight(fun(f1.projection, f2.projection), weights.projection)
    val (selection, selectionW) = withWeight(fun(f1.selection, f2.selection), weights.selection)
    val (groupBy, groupByW) = withWeight(fun(f1.groupBy, f2.groupBy), weights.groupBy)

    val wSum = projectionW + selectionW + groupByW

    if (wSum == 0.0) 0.0
    else (projection * projectionW + selection * selectionW + groupBy * groupByW) * (1.0 / wSum)
  }

  override def measureSimilarity(query1: AnalysedQuery, query2: AnalysedQuery): Double =
    calculate(Features(query1), Features(query2))(calcJaccardSimilarity)

  override def measureDissimilarity(query1: AnalysedQuery, query2: AnalysedQuery): Double =
    calculate(Features(query1), Features(query2))(calcJaccardDissimilarity)
}
