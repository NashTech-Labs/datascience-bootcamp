package controllers

import javax.inject.Inject
import models.{User, UserRepository}
import models.CustomAction
import play.api.data.Forms._
import play.api.data._
import play.api.mvc._
import play.api.libs.ws._
import play.api.libs.json._
import play.api.http.HttpEntity
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import akka.util.ByteString
import play.api.libs.json.JsValue
import views._

import scala.concurrent.{ExecutionContext, Future}


class BodyParserController @Inject()(ws: WSClient,
                                   cc: MessagesControllerComponents)(implicit ec: ExecutionContext)
  extends MessagesAbstractController(cc) {

  def index: Action[AnyContent] = Action { implicit request =>
    Ok(views.html.index())
  }
  
  def bodyParser = Action { request: Request[AnyContent] =>
    val body: AnyContent = request.body
    val jsonBody: Option[JsValue] = body.asJson

    // Expecting json body
    jsonBody.map { json =>
      Ok("Got: " + (json \ "key1").as[String])
    }.getOrElse {
      BadRequest("Expecting application/json request body")
    }
  }


}




