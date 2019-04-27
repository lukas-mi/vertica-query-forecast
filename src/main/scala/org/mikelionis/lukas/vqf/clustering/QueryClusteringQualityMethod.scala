package org.mikelionis.lukas.vqf
package clustering

import query._

trait QueryClusteringQualityMethod {
  def measureClusteringQuality(clusters: Vector[List[Query]]): Double
}
