package org.mikelionis.lukas.vqf

import similarity._
import clustering._
import model._
import fs._
import http._
import formatting._
import logging._

import akka.http.scaladsl.model.Uri
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import scopt.OParser

import java.io.File

object Main extends App with Logging {
  case class MainConfig(
      queriesPath: File = new File("."),
      users: Seq[String] = Seq.empty,
      weights: Weights = Weights.default,
      port: Int = 8000,
      vqAnalyserEndpoint: Uri = Uri.Empty
  )

  def prettyConfig(config: MainConfig): String =
    Seq(
      "using config:",
      s"--queries-path=${config.queriesPath}",
      s"--users=${config.users.mkString(",")}",
      s"--weights=projection=${config.weights.projection},selection=${config.weights.selection},groupBy=${config.weights.groupBy}",
      s"--port=${config.port}",
      s"--vq-analyser-endpoint=${config.vqAnalyserEndpoint.toString}"
    ).mkString("\n") + "\n"

  override def main(args: Array[String]): Unit = {
    val builder = OParser.builder[MainConfig]
    val parser = {
      import builder._

      OParser.sequence(
        programName("vq-forecast"),
        opt[File]("queries-path")
          .required()
          .text("Directory path with queries")
          .validate(f => if (f.isDirectory) success else failure("specified path is not a directory"))
          .action((a, c) => c.copy(queriesPath = a)),
        opt[Seq[String]]("users")
          .required()
          .text("Users whose queries will be used to build models")
          .action((a, c) => c.copy(users = a.distinct)),
        opt[Map[String, Double]]("weights")
          .required()
          .text("Weights use for query similarity")
          .action((a, c) => c.copy(weights = Weights(a("projection"), a("selection"), a("groupBy")))),
        opt[Int]("port")
          .text("Port to run the service on")
          .optional()
          .action((a, c) => c.copy(port = a)),
        opt[String]("vq-analyser-endpoint")
          .required()
          .text("Vertica query analyser endpoint")
          .action((a, c) => c.copy(vqAnalyserEndpoint = Uri(a)))
      )
    }

    val config = OParser.parse(parser, args, MainConfig()) match {
      case Some(c) => c
      case None =>
        log.error("could not parse args")
        sys.exit(1)
    }

    log.info(prettyConfig(config))

    val queriesByUser = QueryReader
      .readQueries(config.queriesPath)
      .filter(q => config.users.contains(q.metadata.user))
      .groupBy(_.metadata.user)
      .mapValues(_.toList)

    log.info("user queries:\n" + FormattingUtils.prettyPairs(queriesByUser.mapValues(_.size)) + "\n")

    val querySimilarityMethod = new AligonMethod(config.weights)
    val queryClusteringMethod = new DensityBasedClusteringMethod(querySimilarityMethod, 5, 25)
    val modelFactory = new MarkovModelFactory(querySimilarityMethod, queryClusteringMethod, 1)
    val modelsByUser = queriesByUser.map { case (user, userQueries) => user -> modelFactory.createModel(userQueries) }

    log.info("created models")

    implicit val system = ActorSystem("vq-forecast")
    implicit val materializer = ActorMaterializer()
    implicit val executionContext = system.dispatcher

    val server = new QueryForecastService(modelsByUser, config.vqAnalyserEndpoint)

    val bindingFuture = Http().bindAndHandle(server.routes, "0.0.0.0", config.port)
    log.info(s"started server on port ${config.port}")

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        log.info("shutting down")
        bindingFuture
          .flatMap(_.unbind())
          .onComplete(_ => system.terminate())
      }
    })

  }
}
