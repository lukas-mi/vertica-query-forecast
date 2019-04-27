package org.mikelionis.lukas.vqf
package model

import query._

trait QueryForecastModel {
  def forecastQueries(query: AnalysedQuery): List[Query]
}
