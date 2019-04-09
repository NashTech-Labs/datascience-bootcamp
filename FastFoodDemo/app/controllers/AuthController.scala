package controllers

import java.security.MessageDigest

import javax.inject.Inject
import models.{FoodRepository, OrderRepository, UserRepository}
import play.api.data._
import play.api.i18n._
import play.api.mvc._

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

class AuthController @Inject()(securityService: Security, userService: UserRepository, foodService: FoodRepository, cc: MessagesControllerComponents)(implicit ec: ExecutionContext) extends MessagesAbstractController(cc) {
  import LoginForm._
  import MenuForm._

  private val postUrl = routes.OrderController.order()

  def index = Action {
    Ok(views.html.index())
  }

  def getSha(str: String): String= {
    MessageDigest.getInstance("SHA-256")
      .digest(str.getBytes("UTF-8")).map("%02x".format(_)).mkString
  }



  def auth() = Action.async { implicit request =>
    val failFunc=null

    val successFunc= { form: Data=>
      {
        val shaStr=getSha(form.password)
        //val foodForm=foodService.getFoodForm()
        val result=Ok(views.html.menu(postUrl, menuForm)).withSession("USERNAME" -> form.name, "PASS" -> shaStr)
        securityService.security(form.name, shaStr, result)
      }
    }

    loginForm.bindFromRequest.fold(failFunc, successFunc)
  }


}


