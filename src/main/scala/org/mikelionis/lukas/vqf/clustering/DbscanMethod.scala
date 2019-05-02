package org.mikelionis.lukas.vqf
package clustering

import query._
import similarity._

import scala.collection.mutable
import scala.collection.parallel.ParMap

class DbscanMethod(
    eps: Double,
    minNeighborSize: Int,
    withNoise: Boolean,
    similarityMethod: QuerySimilarityMethod
) extends QueryClusteringMethod {
  require(eps > 0.0 && eps < 1.0, "eps has to be in (0.0;1.0) range")

  private object Status extends Enumeration {
    type Status = Status.Value
    val UNVISITED, VISITED, NOISE = Value
  }

  private case class State(status: Status.Status = Status.UNVISITED, clusterId: Int = -1)

  private val stateMap = mutable.Map.empty[Int, State].withDefaultValue(State())

  private def dbscan(queries: Iterable[Query]): Unit = {
    val neighborhood = calcNeighborhood(queries)
    var clusterId = 0

    queries.foreach { query =>
      val status = stateMap(query.metadata.epoch).status
      if (status == Status.UNVISITED) {
        val neighbors = neighborhood(query.metadata.epoch)
        if (neighbors.size < minNeighborSize) {
          stateMap(query.metadata.epoch) = State(Status.NOISE)
        } else {
          clusterId = expand(query, neighbors, neighborhood, clusterId)
        }
      }
    }
  }

  private def calcNeighborhood(queries: Iterable[Query]): ParMap[Int, Seq[Query]] =
    queries.par
      .map(query => query.metadata.epoch -> findNeighbors(query, queries))
      .toMap

  private def findNeighbors(query: Query, otherQueries: Iterable[Query]): Seq[Query] =
    otherQueries.foldLeft(List.empty[Query]) {
      case (accNeighbors, potentialNeighbor) =>
        if (query.metadata.epoch != potentialNeighbor.metadata.epoch) {
          if (similarityMethod.measureDissimilarity(query.analysed, potentialNeighbor.analysed) > eps) accNeighbors
          else potentialNeighbor :: accNeighbors
        } else accNeighbors
    }

  private def expand(elem: Query, neighbors: Seq[Query], neighborhood: ParMap[Int, Seq[Query]], clusterId: Int): Int = {
    stateMap(elem.metadata.epoch) = State(Status.VISITED, clusterId)
    val queue = new mutable.Queue[Query]()
    queue ++= neighbors

    while (queue.nonEmpty) {
      val neighbor = queue.dequeue
      val status = stateMap(neighbor.metadata.epoch).status

      if (status == Status.NOISE) {
        stateMap(neighbor.metadata.epoch) = State(Status.VISITED, clusterId)
      } else if (status == Status.UNVISITED) {
        stateMap(neighbor.metadata.epoch) = State(Status.VISITED, clusterId)
        val neighborNeighbors = neighborhood(neighbor.metadata.epoch)
        if (neighborNeighbors.size >= minNeighborSize) {
          queue ++= neighborNeighbors
        }
      }
    }

    clusterId + 1
  }

  override def clusterQueries(queries: List[Query]): Vector[List[Query]] = {
    dbscan(queries)

    val clusters = queries
      .map(query => stateMap(query.metadata.epoch).clusterId -> query)
      .groupBy { case (clusterId, _) => clusterId }
      .toVector
      .sortBy { case (clusterId, _) => clusterId }
      .map { case (_, clusterQueries) => clusterQueries.map(_._2) }

    if (withNoise || clusters.size < 2) clusters else clusters.tail
  }
}
