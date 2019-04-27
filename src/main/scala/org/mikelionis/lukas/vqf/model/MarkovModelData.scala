package org.mikelionis.lukas.vqf
package model

import query._
import similarity._

case class MarkovModelData(
    clusters: Vector[List[Query]],
    probabilityMatrix: Vector[Vector[Double]],
    querySimilarityMethod: QuerySimilarityMethod
)
