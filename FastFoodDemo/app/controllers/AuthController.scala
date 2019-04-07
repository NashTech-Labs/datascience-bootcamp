package controllers

import java.security.MessageDigest

import javax.inject.Inject
import models.UserRepository
import play.api.data._
import play.api.i18n._
import play.api.mvc._

import scala.concurrent.ExecutionContext

class AuthController @Inject()(userService: UserRepository, cc: MessagesControllerComponents)(implicit ec: ExecutionContext) extends MessagesAbstractController(cc) {
  import LoginForm._
  import MenuForm._

  private val postUrl = routes.OrderController.order()

  def index = Action {
    Ok(views.html.index())
  }

  def auth() = Action.async { implicit request =>
    val failFunc=null

    val successFunc= { form: Data=>
      {
        val shaStr=MessageDigest.getInstance("SHA-256").digest(form.password
          .getBytes("UTF-8")).map("%02x".format(_)).mkString
        userService.checkUser(form.name, shaStr).map { check =>
          println(shaStr)
          if (check==Some(1)) {
            Ok(views.html.menu(postUrl, menuForm)).withSession("USERNAME" -> form.name, "PASS" -> shaStr)
          }
          else { Ok(views.html.index()) }
        }
      }
    }

    loginForm.bindFromRequest.fold(failFunc, successFunc)

  }


}


