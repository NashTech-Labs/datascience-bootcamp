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
  /*
  val foodItemsList=List("Cheese Burger", "Double Double")
  val foodItems=tuple("Cheese Buger" -> number, "Double Double" -> number, "Fries" -> number)
  val foodItems=tuple(foodItemsList.map( x => x) )

  val menuFormTupe = Form(
    //foodItems.map{ foodItem => foodItem -> number(min = 0) }
    foodItems
  )
  */
}
