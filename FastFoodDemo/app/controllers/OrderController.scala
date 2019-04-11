package controllers

import javax.inject.Inject
import models.{OrderRepository, SecurityAction, UserRepository}
import play.api.data._
import play.api.i18n._
import play.api.mvc._

import play.api.Play.current
import play.api.i18n.Messages.Implicits._

import scala.concurrent.{ExecutionContext, Future}

class OrderController @Inject()(messagesApi: MessagesApi, securityAction: SecurityAction, userService: UserRepository, orderService: OrderRepository,
                                cc: MessagesControllerComponents)(implicit ec: ExecutionContext) extends MessagesAbstractController(cc) with I18nSupport {
  import MenuForm._




  def index = Action {
    Ok(views.html.index())
  }


  def order = securityAction.async { implicit request =>
    val name=request.session.get("USERNAME")

    /*
    val failFunc={
      formWithErrors: Form[MenuData] => {
        Future(Ok(views.html.menu(routes.OrderController.order, formWithErrors)))
      }
    }
    */

    val failFunc=null

    val successFunc={
      orderForm: MenuData => {
        val orderId=scala.util.Random.nextInt
        orderService.insert(orderForm, orderId, name)
        Future(Ok(views.html.order(orderForm)))
      }
    }

    menuForm.bindFromRequest.fold(failFunc, successFunc)
  }


}

