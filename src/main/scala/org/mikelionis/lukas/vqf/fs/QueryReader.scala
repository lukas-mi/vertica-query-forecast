package org.mikelionis.lukas.vqf
package fs

import query._
import serialization.JsonProtocol._

import java.io.File
import scala.util.{Failure, Success, Try}

object QueryReader {

  private def matchesEndWithoutEpoch(file: File, epoch: Int, end: String): Boolean =
    file.getName.replaceFirst(epoch.toString, "") == end

  private def groupFilesByEpoch(files: Array[File]): Map[Int, Array[File]] = {
    def updateMap(acc: Map[Int, Array[File]], currentFile: File): Map[Int, Array[File]] =
      Try(currentFile.getName.take(8).toInt) match {
        case Success(epoch) =>
          val previousFiles = acc.getOrElse(epoch, Array.empty)
          val updatedEntry = epoch -> (previousFiles :+ currentFile)
          acc + updatedEntry
        case Failure(_) => acc
      }

    files.foldLeft(Map.empty[Int, Array[File]])(updateMap)
  }

  private def readQuery(epoch: Int, files: Array[File]): Option[Query] =
    for {
      analysedQueryFile <- files.find(file => matchesEndWithoutEpoch(file, epoch, ".sql.json"))
      metadataFile <- files.find(file => matchesEndWithoutEpoch(file, epoch, ".json"))
      queryFile <- files.find(file => matchesEndWithoutEpoch(file, epoch, ".sql"))
      analysedQuery <- Try(FileUtils.readJSON[Seq[AnalysedQuery]](analysedQueryFile).head).toOption
      queryMetadata <- Try(FileUtils.readJSON[QueryMetadata](metadataFile)).toOption
    } yield Query(queryFile, analysedQuery, queryMetadata)

  def readQueries(path: File): Iterable[Query] =
    if (path.isDirectory) {
      val (files, directories) = path.listFiles().partition(_.isFile)

      val queries = for {
        (epoch, epochFiles) <- groupFilesByEpoch(files)
        query <- readQuery(epoch, epochFiles)
      } yield query

      if (directories.isEmpty) queries
      else queries ++ directories.map(readQueries).reduce(_ ++ _)
    } else Iterable.empty
}
