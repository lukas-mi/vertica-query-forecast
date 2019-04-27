package org.mikelionis.lukas.vqf
package http

import query._
import model._
import serialization.JsonProtocol._
import logging._

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.unmarshalling.Unmarshal

import java.nio.file.Files

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class QueryForecastService(userModels: Map[String, QueryForecastModel], vqAnalyserEndpoint: Uri)(
    implicit
    actorSystem: ActorSystem,
    materializer: ActorMaterializer,
    executionContext: ExecutionContext
) extends Logging {

  private case class VQAnalyserException(message: String, status: StatusCode) extends Exception

  private def postQuery(queryText: String): Future[AnalysedQuery] = {
    val responseF = Http()
      .singleRequest(
        HttpRequest(
          uri = vqAnalyserEndpoint,
          method = HttpMethods.POST,
          entity = HttpEntity.apply(queryText.getBytes("UTF-8"))
        )
      )

    responseF.flatMap { response =>
      response.status match {
        case StatusCodes.OK => Unmarshal(response).to[Seq[AnalysedQuery]].map(_.head)
        case notOK => Unmarshal(response).to[String].flatMap(msg => Future.failed(VQAnalyserException(msg, notOK)))
      }
    }
  }

  private def nextQuery(model: QueryForecastModel, queryText: String): Future[String] =
    postQuery(queryText).map { query =>
      model.forecastQueries(query) match {
        case headQuery :: _ =>
          val nextQueryBytes = Files.readAllBytes(headQuery.file.toPath)
          new String(nextQueryBytes, "UTF-8")
        case Nil => ""
      }
    }

  val routes: Route = pathPrefix("forecast") {
    path("users" / Segment) { user =>
      post {
        decodeRequest {
          entity(as[String]) { queryText =>
            userModels.get(user) match {
              case Some(model) =>
                onComplete(nextQuery(model, queryText)) {
                  case Success(nextQueryText) => complete(nextQueryText)
                  case Failure(VQAnalyserException(msg, status)) => complete(status, msg)
                  case Failure(ex) =>
                    log.error(ex)("failed to forecast query")
                    complete(StatusCodes.InternalServerError)
                }
              case None => complete(StatusCodes.NotFound, s"user '$user' not found")
            }
          }
        }
      }
    }
  }

}
