# --- First database schema

# --- !Ups

set ignorecase true;


create table users (
  id                        bigint not null,
  name                      varchar(255) not null,
  pass                      varchar(511) not null,
  constraint pk_users primary key (id))
;

create table food_orders (
  id                        bigint not null auto_increment,
  food_order_id             bigint not null,
  user_id                   varchar(255) not null,
  food_item                 varchar(255) not null,
  price                     bigint not null,
  quantity                  bigint not null,
  constraint pk_food_orders primary key(id)
);

# --- !Downs

drop table users;

drop table food_orders;
