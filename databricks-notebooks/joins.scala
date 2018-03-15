// Databricks notebook source
val customers = sc.parallelize(Array[(String, Option[Int])](
  ("Müller", Some(1)), ("Meyer", Some(2)), ("Schulze", Some(3)), ("Schmidt", Some(4))))
  .toDF("lastName", "customerId")

val orders = sc.parallelize(Array[(String, Option[Int], Option[Int])](
  ("2019-01-01", Some(1), Some(1)), ("2019-01-02", Some(2), Some(2)), ("2019-01-03", Some(3), Some(3)), ("2019-01-04", Some(4), Some(1)), ("2019-01-04", Some(5), null) ))
  .toDF("date", "orderId", "customerId")

val orderItems = sc.parallelize(Array[(String, Option[Int])](
  ("Bier", Some(1)), ("Wein", Some(2)), ("Wein", Some(3)), ("Wein", Some(3)) ))
  .toDF("product", "orderId")

customers.createOrReplaceTempView("customers")
orders.createOrReplaceTempView("orders")
orderItems.createOrReplaceTempView("order_items")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * 
// MAGIC from customers
// MAGIC cross join orders o  

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from customers c
// MAGIC inner join orders o
// MAGIC   on o.customerId = c.customerId
// MAGIC   
// MAGIC -- Schmidt is missing

// COMMAND ----------

// MAGIC %sql 
// MAGIC select c.* 
// MAGIC from customers c
// MAGIC left join orders o
// MAGIC   on o.customerId = c.customerId
// MAGIC where orderId is null
// MAGIC 
// MAGIC -- Schmidt is back!

// COMMAND ----------

// MAGIC %sql 
// MAGIC select * from customers c
// MAGIC right join orders o
// MAGIC   on o.customerId = c.customerId
// MAGIC 
// MAGIC -- We see the order with no customer assigned

// COMMAND ----------

// MAGIC %sql 
// MAGIC select * from customers c
// MAGIC full outer join orders o
// MAGIC   on o.customerId = c.customerId
// MAGIC   
// MAGIC -- We see Schmidt AND the order with customer

// COMMAND ----------

// MAGIC %sql 
// MAGIC select * from customers c
// MAGIC left semi join orders o
// MAGIC   on o.customerId = c.customerId
