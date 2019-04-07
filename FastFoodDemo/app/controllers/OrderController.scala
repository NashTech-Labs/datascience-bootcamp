package controllers

import javax.inject.Inject
import models.{OrderRepository, UserRepository}
import play.api.data._
import play.api.i18n._
import play.api.mvc._

import scala.concurrent.ExecutionContext

class OrderController @Inject()(userService: UserRepository, orderService: OrderRepository,
                                cc: MessagesControllerComponents)(implicit ec: ExecutionContext) extends MessagesAbstractController(cc) {
  import MenuForm._




  def index = Action {
    Ok(views.html.index())
  }

  /*
  def order = Action.async { implicit request =>
    menuForm.bindFromRequest.fold( null,
      orderForm => {
        val orderId=scala.util.Random.nextInt
        println("session2 " + request.session.get("USERNAME"))
        orderService.insert(orderForm, orderId).map { _ =>
          if (request.session.get("USERNAME")==Some("jouko")) {
            Ok(views.html.order(orderForm, "jouko", "123"))
          } else {
            Ok(views.html.index())
          }
        }
      }
    )
  }
  */

  /*
  val name=request.session.get("USERNAME")
  val pass=request.session.get("PASS")
  val check= (name, pass) match {
    case (Some(name), Some(pass))  => userService.checkUser(name, pass)
    case _ => 0
  }
  */

  /*
  def order = Action.async { implicit request =>
    menuForm.bindFromRequest.fold( null,
      orderForm => {
        val orderId=scala.util.Random.nextInt
        orderService.insert(orderForm, orderId).map { _ =>
          val name=request.session.get("USERNAME")
          val pass=request.session.get("PASS")
          (name, pass) match {
            case (Some(nameStr), Some(passStr))  => userService.checkUser(nameStr, passStr).map { _ => Ok(views.html.order(orderForm)) }
            case _ => Ok(views.html.index())
          }
        }
      }
    )
  }
  */

  def order = Action.async { implicit request =>
    val name=request.session.get("USERNAME")
    val pass=request.session.get("PASS")
    val check= (name, pass) match {
      case (Some(name), Some(pass))  => userService.checkUser2(name, pass)
      case _ => 0
    }
    println("name= " + name)
    println("pass= " + pass)
    println("check= " + check)
    menuForm.bindFromRequest.fold( null,
      orderForm => {
        val orderId=scala.util.Random.nextInt
        println("session2 " + request.session.get("USERNAME"))
        orderService.insert(orderForm, orderId).map { _ =>
          if (check==Some(1)) {
            Ok(views.html.order(orderForm))
          } else {
            Ok(views.html.index())
          }
        }
      }
    )
  }

}

