package controllers

import models.{Order, OrderRepository}

object MenuForm {
  import play.api.data.Forms._
  import play.api.data.Form

  case class MenuData(cheeseBurger: Int, doubleDouble: Int, fries: Int, milkShake: Int)

  val menuForm = Form(
    mapping(
      "Cheese Burger $2" -> number(min = 0),
      "Double Double $4" -> number(min = 0),
      "Fries $1" -> number(min = 0),
      "Milk Shake $3" -> number(min = 0)
    )(MenuData.apply)(MenuData.unapply)
  )
}
