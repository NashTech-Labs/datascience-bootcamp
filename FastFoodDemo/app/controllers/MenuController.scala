package controllers

import javax.inject.Inject
import play.api.data._
import play.api.i18n._
import play.api.mvc._
import models.{FoodRepository, UserRepository}

class MenuController @Inject()(userService: UserRepository, foodService: FoodRepository, cc: MessagesControllerComponents) extends MessagesAbstractController(cc) {
  import MenuForm._
  import LoginForm._

  private val postUrl = routes.OrderController.order()


  def index = Action {
    Ok(views.html.index())
  }



  def menu = Action { implicit request: MessagesRequest[AnyContent] =>
    val name=request.session.get("USERNAME")
    val pass=request.session.get("PASS")
    val check= (name, pass) match {
      case (Some(name), Some(pass))  => userService.checkUser(name, pass)
      case _ => 0
    }
    //val foodItems=foodService.getFoodItems()
    if (check==Some(1)) {
      Ok(views.html.menu(postUrl, menuForm))
    }
    else {
      Ok(views.html.index())
    }
  }


}
