package controllers

import javax.inject.Inject
import models.{OrderRepository, UserRepository}
import play.api.data._
import play.api.i18n._
import play.api.mvc._

import scala.concurrent.ExecutionContext

class ListController @Inject()(userService: UserRepository, orderService: OrderRepository,
                                cc: MessagesControllerComponents)(implicit ec: ExecutionContext) extends MessagesAbstractController(cc) {
  import MenuForm._




  def index = Action {
    Ok(views.html.index())
  }


  def list = Action.async { implicit request =>
    val name=request.session.get("USERNAME")
    val pass=request.session.get("PASS")
    val check = (name, pass) match {
      case (Some(nameStr), Some(passStr))  => userService.checkUser2(nameStr, passStr)
      case _ => 0
    }


    orderService.getOrders(name).map{ listOrders => if (check==Some(1)) {Ok(views.html.list(listOrders))} else {Ok(views.html.index())} }

  }


}


