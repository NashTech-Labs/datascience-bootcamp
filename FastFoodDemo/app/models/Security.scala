package controllers


import javax.inject.Inject
import models.UserRepository
import play.api.data._
import play.api.i18n._
import play.api.mvc._

import scala.concurrent.ExecutionContext

import scala.concurrent.Future

class Security @Inject()(userService: UserRepository, cc: MessagesControllerComponents)(implicit ec: ExecutionContext) extends MessagesAbstractController(cc) {

  def security(name: String, pass: String, func: Result): Future[Result] = {
    userService.checkUser(name, pass).map { check =>
      println("check= " + check)
      if (check.contains(1)) {
        func
      }
      else { Ok(views.html.index()) }
    }
  }
}



