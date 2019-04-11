package models


import java.util.Date

import javax.inject.Inject
import anorm.SqlParser.{get, scalar}
import anorm._
import controllers.MenuForm.{MenuData}
import play.api.db.DBApi

import scala.concurrent.Future

case class Order(id: Long, food_order_id: Long, user_id: String, item: String, price: Long, quantity: Long)

object Order {
  implicit def toParameters: ToParameterList[Order] =
    Macro.toParameters[Order]
}

@javax.inject.Singleton
class OrderRepository @Inject()(dbapi:DBApi)(implicit ec: DatabaseExecutionContext) {
  private val db = dbapi.database("default")

  private val simple = {
    get[Long]("food_orders.id") ~
      get[Long]("food_orders.food_order_id") ~
      get[String]("food_orders.user_id") ~
      get[String] ("food_orders.food_item") ~
      get[Long] ("food_orders.price" ) ~
      get[Long] ("food_orders.quantity" ) map {
      case id ~ food_order_id ~ user_id ~ item ~ price ~ quantity =>
        Order(id, food_order_id, user_id, item, price, quantity)
    }
  }

  def insert(order: MenuData, orderId: Int, userId: Option[String]): Future[Option[Long]] = Future {
    val name= userId match { case Some(userIdStr) => userIdStr; case _ => " "}
    db.withConnection { implicit connection =>
      SQL"insert into food_orders (food_order_id, user_id, food_item, price, quantity) values ($orderId, $name, 'Cheese Burger', '2', ${order.cheeseBurger}) ".executeInsert()
      SQL"insert into food_orders (food_order_id, user_id, food_item, price, quantity) values ($orderId, $name, 'Double Double', '4', ${order.doubleDouble}) ".executeInsert()
      SQL"insert into food_orders (food_order_id, user_id, food_item, price, quantity) values ($orderId, $name, 'Fries', '1', ${order.fries}) ".executeInsert()
      SQL"insert into food_orders (food_order_id, user_id, food_item, price, quantity) values ($orderId, $name, 'Milk Shake', '3', ${order.milkShake}) ".executeInsert()
    }
  }(ec)

  def getOrders(userId: Option[String]): Future[List[Order]] = Future {
    val name= userId match { case Some(userIdStr) => userIdStr; case _ => " "}
    db.withConnection { implicit connection =>
      SQL"select * from food_orders where user_id=$name".as(simple.*)
    }
  }(ec)

  def getOrders(userId: Option[String], foodItem: String, minQuantity: Int): Future[List[Order]] = Future {
    val name= userId match { case Some(userIdStr) => userIdStr; case _ => ""}
    //val foodItemStr=foodItem match { case Some(foodItemStr) => foodItemStr; case _ => " " }
    val queryFood=if (foodItem!="") { " and food_item='" + foodItem + "'" } else { "" }
    val queryQuantity=" and quantity>=" + minQuantity
    val query="select * from food_orders where user_id='" + name + "'" + queryFood + queryQuantity
    println(query)
    db.withConnection { implicit connection =>
      //SQL"select * from food_orders where user_id=$name and food_item=$foodItemStr".as(simple.*)
      SQL(query).as(simple.*)
    }
  }(ec)

}
