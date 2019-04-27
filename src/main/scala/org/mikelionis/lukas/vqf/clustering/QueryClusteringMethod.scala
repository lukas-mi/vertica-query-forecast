package org.mikelionis.lukas.vqf
package clustering

import query._

trait QueryClusteringMethod {
  def clusterQueries(queries: List[Query]): Vector[List[Query]]
}
