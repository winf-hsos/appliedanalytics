// Databricks notebook source
// MAGIC %md
// MAGIC # Beispiele aus dem 2. Termin
// MAGIC Für euch zum nachlesen, die Beispiele die wir uns im 2. Termin angesehen haben.

// COMMAND ----------

// MAGIC %md
// MAGIC ## 1. Zu euren Fragen

// COMMAND ----------

// MAGIC %sql
// MAGIC -- Alles was in der Klammer steht mit inst und substr
// MAGIC select 
// MAGIC       grammage
// MAGIC      ,instr(grammage, "(") as `Position erste Klammer`
// MAGIC      ,instr(grammage, ")") as `Position zweite Klammer`
// MAGIC      ,instr(grammage, ")") - instr(grammage, "(") 
// MAGIC          as `Anzahl Zeichen zwischen den Klammern`
// MAGIC      , substr( grammage 
// MAGIC         ,instr(grammage, "(") + 1
// MAGIC         ,instr(grammage, ")") - instr(grammage, "(") - 1
// MAGIC        ) as `Zeichen zwischen den Klammern`
// MAGIC from products

// COMMAND ----------

// MAGIC %sql
// MAGIC select grammage
// MAGIC       ,locate("g", grammage, 
// MAGIC        locate("g", grammage) + 1) 
// MAGIC           as `Position des zweiten g`
// MAGIC from products

// COMMAND ----------

// MAGIC %sql
// MAGIC -- Gibt die Länge des Spalteninhalts zurück
// MAGIC select length(productDescription) as `Länge Produktbeschreibung`
// MAGIC from products

// COMMAND ----------

// MAGIC %md
// MAGIC ### Laden einer externen Tabelle
// MAGIC Die Datei muss im Web via URL zugreifbar sein.

// COMMAND ----------

// MAGIC %fs rm -r FileStore/tables/fuellwoerter.csv

// COMMAND ----------


/* Laden der Füllwörter aus Amazon S3 */
val url = "https://s3.amazonaws.com/nicolas.meseth/data+sets/fuellwoerter.txt"
val file = scala.io.Source.fromURL(url).mkString
val list = file.split("\n").filter(_ != "")
val rdd = sc.parallelize(list)
rdd.coalesce(1).saveAsTextFile("FileStore/tables/fuellwoerter.csv")

/* Dataframe erstellen */
val words = spark.read.option("header", "false") 
                      .option("inferSchema", "true")
                      .csv("FileStore/tables/fuellwoerter.csv")
words.unpersist()
words.cache()

// Temporären View erstellen ...
words.createOrReplaceTempView("fillwords");
// ... oder eine globale permanente Tabelle
//words.write.saveAsTable("fillwords");


// COMMAND ----------

// MAGIC %sql
// MAGIC -- Zum Test eine Abfrage
// MAGIC select * from fillwords

// COMMAND ----------

// MAGIC %md
// MAGIC ## 2. Tipps

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1. Tipp: Verwenden von DISTINCT

// COMMAND ----------

// MAGIC %sql
// MAGIC -- Nur eindeutige Werte (keine doppelten)
// MAGIC select distinct productCategory, productSubCategory
// MAGIC from products
// MAGIC order by productCategory

// COMMAND ----------

// MAGIC %sql
// MAGIC -- Alternative mit group by
// MAGIC select productCategory, productSubCategory
// MAGIC from products
// MAGIC group by productCategory, productSubCategory
// MAGIC order by productCategory

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2. Tipp: Verwenden von Markdown in Notebooks
// MAGIC <hr>
// MAGIC Ihr könnt eine sehr schöne Dokumentation inklusive Bilder etc. mittels Markdown erstellen. Leitet einfach einen Codeblock mit `%md` ein und schreibt den Text mit Markdown Syntax.Die Syntaxt ist sehr einfach:
// MAGIC 
// MAGIC 
// MAGIC # # Überschrift 1. Grades
// MAGIC ## ## Überschrit 2. Grades
// MAGIC ### ### Überschrift 3. Grades
// MAGIC 
// MAGIC Ihr könnt auch Text **fett** order *kursiv* hervorheben. Code einbinden funktioniert auch: `select * from products`
// MAGIC 
// MAGIC Bilder einfügen geht so:
// MAGIC 
// MAGIC ![Das Databricks Logo](https://forums.databricks.com/themes/base/images/teamhub-logo.png)
// MAGIC 
// MAGIC Mehr Infos zu Markdown finden ihr [hier](https://forums.databricks.com/static/markdown/help.html).

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3. Tipp: Erstellen von Views
// MAGIC Bestimmte Abfragen, die ihr immer wieder benötigt, kann man sinnvollerweise in einem View abbilden. Das erlaubt es die Abfrage wie eine Tabelle in einer anderen Abfrage zu verwenden.

// COMMAND ----------

// MAGIC %sql
// MAGIC -- Erstellen eines View
// MAGIC create or replace view products_non_food as
// MAGIC select gtin
// MAGIC       ,productName
// MAGIC       ,productCategory
// MAGIC       ,productSubCategory
// MAGIC       ,productType
// MAGIC       ,price
// MAGIC       ,grammage
// MAGIC from products
// MAGIC where productCategory IN ('Tier', 'Drogerie und Kosmetik', 'Küche und Haushalt')
// MAGIC and productSubCategory NOT IN ('Babynahrung und Zubehör')

// COMMAND ----------

// MAGIC %sql
// MAGIC -- Den View wie eine Tabelle verwenden
// MAGIC select count(1) from products_non_food

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4. Tipp: Tabellen und Views auflisten

// COMMAND ----------

// MAGIC %sql show tables

// COMMAND ----------

// MAGIC %md
// MAGIC ## 2. SQL Joins

// COMMAND ----------

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
// MAGIC select * from customers

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from orders

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
// MAGIC full outer join orders o
// MAGIC   on o.customerId = c.customerId
// MAGIC   
// MAGIC -- We see Schmidt AND the order with customer

// COMMAND ----------

// MAGIC %sql 
// MAGIC select * from customers c
// MAGIC left semi join orders o
// MAGIC   on o.customerId = c.customerId

// COMMAND ----------

// MAGIC %md 
// MAGIC ## 3. Scala und SQL Hybrid verwenden

// COMMAND ----------

/* Erstellen eines RDD Objekts aus der selben CSV Datei */
var wordsRDD = sc.textFile("FileStore/tables/fuellwoerter.csv");

// COMMAND ----------

/* Zählen der Zeilen */
wordsRDD.count()

// COMMAND ----------

/* Die ersten 20 anzeigen lassen */
wordsRDD.take(20)

// COMMAND ----------

/* Aufteilen der Wörter in einzelne Buchstaben */
val charsRDD = wordsRDD.flatMap(_.split(""))

// COMMAND ----------

/* Die ersten zehn anzeigen */
charsRDD.take(10)

// COMMAND ----------

/* In einem Dataframe umwandeln */
val charDf = charsRDD.toDF("character")

// COMMAND ----------

/* Buchstaben zählen */
val countCharacters = charDf.groupBy("character").count()

// COMMAND ----------

/* Ergebnis anzeigen */
display(countCharacters)

// COMMAND ----------

/* Das ganze jetzt in SQL */
charDf.createOrReplaceTempView("characters")

// COMMAND ----------

val countCharacterWithSql = sqlContext.sql("select character, count(1) as anzahl from characters group by character")

// COMMAND ----------

/* Es geht auch über meherer Zeilen */
val countCharacterWithSql = sqlContext.sql("""
  select 
    character
   ,count(1) as anzahl 
  from characters 
  group by character
""")

// COMMAND ----------

// MAGIC %sql
// MAGIC -- Oder direkt mittels SQL
// MAGIC select 
// MAGIC     character
// MAGIC    ,count(1) as anzahl 
// MAGIC from characters 
// MAGIC group by character
// MAGIC order by anzahl desc

// COMMAND ----------

display(countCharacterWithSql)

// COMMAND ----------

/* Und daraus können wir wieder eine Tabelle machen */
countCharacterWithSql.createOrReplaceTempView("characterCount")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from characterCount
