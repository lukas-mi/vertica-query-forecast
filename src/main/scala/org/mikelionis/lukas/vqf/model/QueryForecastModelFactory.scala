package org.mikelionis.lukas.vqf
package model

import query._

import java.io.File

trait QueryForecastModelFactory[T <: QueryForecastModel] {
  def createModel(queries: List[Query]): T
  def saveModel(file: File, model: T): Unit
  def loadModel(file: File): T
}
