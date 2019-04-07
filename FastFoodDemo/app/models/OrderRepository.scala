package models


import java.util.Date

import javax.inject.Inject
import anorm.SqlParser.{get, scalar}
import anorm._
import controllers.MenuForm.{MenuData}
import play.api.db.DBApi

import scala.concurrent.Future


//#Bad schema but easier to implement for now
//create table orders (
//id                      bigint not null,
//user_id                 varchar(255) not null,
//cheese_burger           bigint not null,
//double_double           bigint not null,
//fries                   bigint not null,
//milk_shake              bigint not null,
//constraint pk_orders primary key(id)
//);

//case class Order(id: Long, user_id: String, cheese_burger: Long, double_double: Long, fries: Long, milk_shake: Long)
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

  def insert(order: MenuData, orderId: Int): Future[Option[Long]] = Future {
    db.withConnection { implicit connection =>
      SQL"insert into food_orders (food_order_id, user_id, food_item, price, quantity) values ($orderId, 'jouko', 'cheese_burger', '2', ${order.cheeseBurger}) ".executeInsert()
      SQL"insert into food_orders (food_order_id, user_id, food_item, price, quantity) values ($orderId, 'jouko', 'double_double', '4', ${order.doubleDouble}) ".executeInsert()
      SQL"insert into food_orders (food_order_id, user_id, food_item, price, quantity) values ($orderId, 'jouko', 'fries', '1', ${order.fries}) ".executeInsert()
      SQL"insert into food_orders (food_order_id, user_id, food_item, price, quantity) values ($orderId, 'jouko', 'milk_shake', '3', ${order.milkShake}) ".executeInsert()
    }
  }(ec)



}
