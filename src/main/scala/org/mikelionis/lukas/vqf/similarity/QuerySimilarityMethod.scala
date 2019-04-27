package org.mikelionis.lukas.vqf
package similarity

import query._

trait QuerySimilarityMethod {
  def measureSimilarity(query1: AnalysedQuery, query2: AnalysedQuery): Double
  def measureDissimilarity(query1: AnalysedQuery, query2: AnalysedQuery): Double
}
