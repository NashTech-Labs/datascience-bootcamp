package controllers

import javax.inject.Inject
import models.{OrderRepository, UserRepository}
import play.api.data._
import play.api.i18n._
import play.api.mvc._

import scala.concurrent.ExecutionContext

class ListController @Inject()(userService: UserRepository, orderService: OrderRepository, security: Security,
                                cc: MessagesControllerComponents)(implicit ec: ExecutionContext) extends MessagesAbstractController(cc) {

  import FilterForm._



  def index = Action {
    Ok(views.html.index())
  }

  def list(foodItem: String, minQuantity: Int) = Action.async { implicit request =>
    val check=userService.checkUser(request)
    val name=request.session.get("USERNAME")


    orderService.getOrders(name, foodItem, minQuantity).map{ listOrders => if (check.contains(1)) {Ok(views.html.list(listOrders, filterForm))} else {Ok(views.html.index())} }
  }

  def list = Action.async { implicit request =>
    val check=userService.checkUser(request)
    val name=request.session.get("USERNAME")


    orderService.getOrders(name).map{ listOrders => if (check.contains(1)) {Ok(views.html.list(listOrders, filterForm))} else {Ok(views.html.index())} }
  }


}


