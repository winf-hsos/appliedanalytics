// Databricks notebook source
// MAGIC %md
// MAGIC # Infos zum Template - Phase 2
// MAGIC Dies ist eine Vorlage für die Phase 2 der Fallstudie. Diese Vorlage ist zwingend zu verwenden, zumindest für die finale Abgabe der Lösungen.
// MAGIC 
// MAGIC In der zweiten Phase wählt ihr aus 3 Custern Aufgaben mit verschiedenen Komplexitätspunkten (= maximale erreichbare Punkte) aus. In Summe müsst ihr mit den ausgewählten Aufgaben **mindestens 60 (mögliche) Komplexitätspunkte** erreichen. In dieser Vorlage sind alle Aufgaben enthalten. Aufgaben die ihr nicht bearbeitet, könnt ihr einfach aus dem Template entfernen. Alternativ gebt ihr einfach im Kommentar an, dass ihr diese Aufgabe nicht bearbeitet. **Jede Aufgabe, für die ein Lösungsansatz abgegeben wird, wird auch bewertet**.
// MAGIC 
// MAGIC Unter jeder Frage ist in dieser Vorlage jeweils 1 Codeblock eingefügt. Das bedeutet nicht, dass ihr nur einen Codeblock verwenden dürft. Fügt so viele darunter ein, wie ihr benötigt. Stellt aber sicher, dass erkennbar ist zu welcher Aufgabe ein Codeblock gehört, indem er als Nachfolger des Blocks mit der Überschrift erscheint.
// MAGIC 
// MAGIC Sollet ihr Visualisierungen verwenden, so lasst diese in der von euch erstellten Form als Ergebnis im Notebook bestehen, so dass sie mit exportiert werden. Ein Neuausführen kann dazu führen, dass Formatierungen oder Einstellungen der Grafik verändert werden.
// MAGIC 
// MAGIC Bitte beachtet sämtliche Informationen im <a href="https://docs.google.com/document/d/1PBpaL1v7__gg1LQkPJ9iKQD10cFU0yVwU6rKW3FK_Ko/edit?usp=sharing" target="_blank">Dokument zum Ablauf und Bewertung der Fallstudie</a>.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Anlegen der Daten
// MAGIC Die folgenden Codeblöcke müssen nacheinander ausgeführt werden, um die Daten einmalig anzulegen. Es ist kein manueller Upload notwendig.
// MAGIC 
// MAGIC Um alle Datensätze hintereinander anzulegen, klickt einfach auf den **"Run all"** button in der oberen Navigationsleiste!

// COMMAND ----------

// MAGIC %fs rm -r FileStore/tables/customers.csv

// COMMAND ----------

/* Laden der Kunden aus Amazon S3 */
val file = scala.io.Source.fromURL("https://s3.amazonaws.com/nicolas.meseth/data+sets/ss2018/fallstudie/customers.csv").mkString
val list = file.split("\n").filter(_ != "")
val rdd = sc.parallelize(list)
rdd.coalesce(1).saveAsTextFile("FileStore/tables/customers.csv")

/* Tabelle customer erstellen */
val customers = spark.read.option("header", "true") 
                      .option("inferSchema", "true")
                      .csv("FileStore/tables/customers.csv")
customers.unpersist()
customers.cache()
customers.createOrReplaceTempView("customers");

// COMMAND ----------

// MAGIC %fs 
// MAGIC rm -r FileStore/tables/orders.csv

// COMMAND ----------

/* Laden der Bestellungen aus Amazon S3 */
val file = scala.io.Source.fromURL("https://s3.amazonaws.com/nicolas.meseth/data+sets/ss2018/fallstudie/orders.csv").mkString
val list = file.split("\n").filter(_ != "")
val rdd = sc.parallelize(list)
rdd.coalesce(1).saveAsTextFile("FileStore/tables/orders.csv")

/* Tabelle orders erstellen */
val orders = spark.read.option("header", "true") 
                      .option("inferSchema", "true")
                      .csv("FileStore/tables/orders.csv")
orders.unpersist()
orders.cache()
orders.createOrReplaceTempView("orders");

// COMMAND ----------

// MAGIC %fs 
// MAGIC rm -r FileStore/tables/order_items.csv

// COMMAND ----------

/* Laden der Bestellpoisitionen aus Amazon S3 */
val file = scala.io.Source.fromURL("https://s3.amazonaws.com/nicolas.meseth/data+sets/ss2018/fallstudie/order_items.csv").mkString
val list = file.split("\n").filter(_ != "")
val rdd = sc.parallelize(list)
rdd.coalesce(1).saveAsTextFile("FileStore/tables/order_items.csv")

/* Tabelle orders erstellen */
val orderItems = spark.read.option("header", "true") 
                      .option("inferSchema", "true")
                      .csv("FileStore/tables/order_items.csv")

orderItems.unpersist()
orderItems.cache()
orderItems.createOrReplaceTempView("order_items");

// COMMAND ----------

// MAGIC %fs
// MAGIC rm -r FileStore/tables/products.csv

// COMMAND ----------

/* Laden der Produkte aus Amazon S3 */
val file = scala.io.Source.fromURL("https://s3.amazonaws.com/nicolas.meseth/data+sets/ss2018/fallstudie/products.csv").mkString
val list = file.split("\n").filter(_ != "")
val rdd = sc.parallelize(list)
rdd.coalesce(1).saveAsTextFile("FileStore/tables/products.csv")

/* Tabelle products erstellen */
val products = spark.read.option("header", "true") 
                      .option("inferSchema", "true")
                      .csv("FileStore/tables/products.csv")

products.cache()
products.createOrReplaceTempView("products");

// COMMAND ----------

// MAGIC %md
// MAGIC ## Cluster 1 - Bestellverhalten

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.1: Gibt es Kunden, die noch nie bestellt haben?
// MAGIC ⬤ ⬤

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.2: Wie viele unterschiedliche Produkte hat ein Kunde im Durchschnitt über alle Bestellungen hinweg gekauft?
// MAGIC ⬤ ⬤ ⬤

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.3: Wie ist die Anzahl getätigter Bestellungen pro Kunde verteilt?
// MAGIC ⬤ ⬤ ⬤

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.4: Zu welchen Uhrzeiten bestellen die Kunden? Gibt es einen Unterschied zwischen Frauen und Männern?
// MAGIC ⬤ ⬤ ⬤ ⬤ ⬤

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.5: Führt eine ABC-Analyse der Kunden durch!
// MAGIC ⬤ ⬤ ⬤ ⬤ ⬤&nbsp;&nbsp;&nbsp;&nbsp;⬤ ⬤ ⬤

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.6: Wie wechselfreudig sind die Kunden beim Kauf von Rotwein?
// MAGIC ⬤ ⬤ ⬤ ⬤ ⬤&nbsp;&nbsp;&nbsp;&nbsp;⬤ ⬤ ⬤

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC ### 1.7: Erstellt einen Bericht, der eure Kunden in die Gruppen “wöchentliche Käufer”, “zweiwöchentliche Käufer” (alle zwei Wochen), “monatliche Käufer” und “gelegentliche Käufer” einordnet! 
// MAGIC ⬤ ⬤ ⬤ ⬤ ⬤&nbsp;&nbsp;&nbsp;&nbsp;⬤ ⬤ ⬤ ⬤ ⬤&nbsp;&nbsp;&nbsp;&nbsp;⬤ ⬤ ⬤

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC ## Cluster 2 - Warenkorbanalyse

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC ### 2.1: Wie hoch ist der Anteil an Bioprodukten in einer durchschnittlichen Bestellung?
// MAGIC ⬤ ⬤ ⬤

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC ### 2.2: Wie ist die Verteilung der Bestellsummen?
// MAGIC ⬤ ⬤ ⬤

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC ### 2.3: Klassifiziert die Kunden in die Gruppen _“Bio-Fans”_, _“Bio-Agnostiker”_ und _“Bio-Gegner”_!
// MAGIC ⬤ ⬤ ⬤ ⬤ ⬤

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC ### 2.4: Vergleicht die Anzahl gekaufter Babyprodukte von weiblichen und männlichen Kunden miteinander!
// MAGIC ⬤ ⬤ ⬤ ⬤ ⬤

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC ### 2.5: Welche 10 Kunden erzeugen den höchsten Umsatz mit alkoholischen Getränken? Wie ist deren Umsatz im Vergleich zum Durchschnitt?
// MAGIC ⬤ ⬤ ⬤ ⬤ ⬤

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC ### 2.6: Gibt es Kunden, die bevorzugt vegetarische Produkte kaufen?
// MAGIC ⬤ ⬤ ⬤ ⬤ ⬤&nbsp;&nbsp;&nbsp;&nbsp;⬤ ⬤ ⬤

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC ### 2.7: Wie ist der durchschnittliche Warenkorb zusammengesetzt? Gibt es Unterschiede zwischen männlichen und weiblichen Käufern? Oder zwischen Altersgruppen?
// MAGIC ⬤ ⬤ ⬤ ⬤ ⬤&nbsp;&nbsp;&nbsp;&nbsp;⬤ ⬤ ⬤ ⬤ ⬤&nbsp;&nbsp;&nbsp;&nbsp;⬤ ⬤ ⬤

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC ## Cluster 3 - Cash Cows & Poor Dogs

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3.1: Gibt es Produkte, die nie bestellt wurden?
// MAGIC ⬤ ⬤

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC ### 3.2: Welche sind umsatzstärksten Produktkategorien?
// MAGIC ⬤ ⬤ ⬤

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC ### 3.3: In welchem Monat wurde das meiste Speiseeis verkauft?
// MAGIC ⬤ ⬤ ⬤ ⬤ ⬤

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC ### 3.4: Von wann bis wann wurden Weihnachtsartikel verkauft?
// MAGIC ⬤ ⬤ ⬤ ⬤ ⬤

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC ### 3.5: Welche Biermarken laufen am besten?
// MAGIC ⬤ ⬤ ⬤ ⬤ ⬤

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC ### 3.6: Gemessen an den vorliegenden Bestellungen, wie sind die Marktanteile der Weizenbiere verteilt?
// MAGIC ⬤ ⬤ ⬤ ⬤ ⬤&nbsp;&nbsp;&nbsp;&nbsp;⬤ ⬤ ⬤

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC ### 3.7: Wie verkaufen sich die REWE Eigenmarken im Vergleich mit konkurrierenden Marken anderer Hersteller?
// MAGIC ⬤ ⬤ ⬤ ⬤ ⬤&nbsp;&nbsp;&nbsp;&nbsp;⬤ ⬤ ⬤ ⬤ ⬤&nbsp;&nbsp;&nbsp;&nbsp;⬤ ⬤ ⬤

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC ## TEAMBONUS
// MAGIC Es gibt in Phase 2 die Chance auf einen Bonus von **3 Punkten (+5%)** für jede Gruppe. Um den Bonus zu erhalten müssen **über alle Gruppen betrachtet** alle Aufgaben aus den 3 Clustern  erfolgreich bearbeitet (Ergebnis > 50%) werden. Ist das der Fall so, wird der Bonus jeder Gruppe auf auf das in dieser Phase erreichte Ergebnis hinzuaddiert.
// MAGIC 
// MAGIC Stimmt euch also zwischen den Gruppen ab, um den Bonus mitzunehmen.
