// Databricks notebook source
// MAGIC %md
// MAGIC # SQL Refresh
// MAGIC Die folgenden Aufgaben sollen euren SQL Kenntnissen (wieder) auf die Sprünge helfen!

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1. Welches Produkt hat die GTIN 4000521006549?

// COMMAND ----------

// MAGIC %sql
// MAGIC select gtin, productName
// MAGIC from products
// MAGIC where gtin = '4000521006549'

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2: Wie viele Produkte sind im Sortiment enthalten?

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) as anzahlProdukte
// MAGIC from products
// MAGIC -- Es sind 7953 Produkte im Sortiment enthalten

// COMMAND ----------

// MAGIC %md 
// MAGIC ### 3. Welches Obst hat die meisten Proteine?

// COMMAND ----------

// MAGIC %sql
// MAGIC select productName, proteinInGram
// MAGIC from products
// MAGIC where productType = 'Obst'
// MAGIC order by proteinInGram desc
// MAGIC limit 5
// MAGIC 
// MAGIC /* 
// MAGIC * In der Kategories "Obst" als Unterkategories sind keine Angaben zu Proteinen vorhanden. Die 
// MAGIC * Frage kann deshalb nicht beantwortet werden. Es wurde aber innerhalb der TK-Produkte
// MAGIC * ein Produkttyp "Obst" gefunden, für den Angaben zu Proteinen vorliegen. Demnach hat
// MAGIC * "REWE Beste Wahl Himbeeren tiefgefroeren 750g" mit 1,5 g pro 100g die meisten Proteine
// MAGIC */

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4. Welche drei Kategorien enthalten die meisten Produkte?

// COMMAND ----------

// MAGIC %sql
// MAGIC select productCategory, count(productId) as anzahlProdukte
// MAGIC from products
// MAGIC group by productCategory
// MAGIC order by anzahlProdukte desc
// MAGIC limit 3

// COMMAND ----------

// MAGIC %md
// MAGIC ### 5. Wie ist die prozentuale Verteilung der Produkte auf die Kategorien?

// COMMAND ----------

// MAGIC %sql
// MAGIC select productCategory
// MAGIC       ,round( count(productId) / (
// MAGIC               -- Unterabfrage für die Gesamtanzahl Produkte
// MAGIC               select count(*) as anzahlProdukte
// MAGIC               from products
// MAGIC       ) * 100, 2) as anzahlProdukte
// MAGIC from products
// MAGIC group by productCategory
// MAGIC order by anzahlProdukte desc

// COMMAND ----------

// MAGIC %md 
// MAGIC ### 6. Welches ist das zweitteuerste Craft Bier im Sortiment?

// COMMAND ----------

// MAGIC %sql
// MAGIC -- Diese Abfrage liefert alle Craft Biere sortiert nach dem Preis
// MAGIC -- und erstellt eine Spalte mit dem Rang (=> Window-Function rank)
// MAGIC -- Diese Abfrage wird im nächsten Block als Unterabfrage verwendet 
// MAGIC -- und auf den Rang 2 gefiltert.
// MAGIC select 
// MAGIC   productName
// MAGIC  ,productCategory
// MAGIC  ,productSubCategory
// MAGIC  ,productType
// MAGIC  ,price
// MAGIC  ,rank() over (order by price desc) as rang
// MAGIC from products
// MAGIC where productType = 'Craft Beer'

// COMMAND ----------

// MAGIC %sql
// MAGIC -- Hier wird das Ergebnis von oben als Unterabfrage (Subquery)
// MAGIC -- verwendet und auf Rang = 2 gefiltert
// MAGIC select * from 
// MAGIC ( select 
// MAGIC   productName
// MAGIC  ,productCategory
// MAGIC  ,productSubCategory
// MAGIC  ,productType
// MAGIC  ,price
// MAGIC  ,rank() over (order by price desc) as rang
// MAGIC   from products
// MAGIC   where productType = 'Craft Beer') rankedbeer
// MAGIC where rang = 2

// COMMAND ----------

// MAGIC %md
// MAGIC ### 7. Welche Produkte sind laut Beschreibung exklusiv?

// COMMAND ----------

// MAGIC %sql
// MAGIC select productName, productDescription
// MAGIC from products
// MAGIC where lower(productDescription) like '% exklusiv%'
// MAGIC -- Hier könnte man noch weiter prüfen, auf was sich "exklusiv"
// MAGIC -- innerhalb der Beschreibung tatsächlich bezieht

// COMMAND ----------

// MAGIC %md
// MAGIC ### 8. Erstellt eine Liste aller Whiskeys sortiert nach dem Preis (teuerster oben) und dem relativen Preis im Vergleich zum teuersten im % als Spalte!

// COMMAND ----------

// MAGIC %sql
// MAGIC -- Der Relation zum teuersten Preis wird mit einer Unterabfrage
// MAGIC -- gelöst, die mit max() den höchsten Preis ermittelt.
// MAGIC select 
// MAGIC   productCategory
// MAGIC  ,productName
// MAGIC  ,productCategory
// MAGIC  ,productSubCategory
// MAGIC  ,productType
// MAGIC  ,price
// MAGIC  ,round(price / ( select max(price) from from products where productType = 'Whiskey' ) * 100, 2) as relativePrice
// MAGIC from products
// MAGIC where productType = 'Whiskey'
// MAGIC order by  price desc

// COMMAND ----------

// MAGIC %md
// MAGIC ### 9: Erstellt eine Liste der Biermarken, sortiert nach der Anzahl der Produkte im Sortiment!

// COMMAND ----------

// MAGIC %sql
// MAGIC -- Annahme: Biermischgetränke sowie Craft Beer zählen mit zu den 
// MAGIC -- Biermarken und sind deshalb im Ergebnis enthalten
// MAGIC select 
// MAGIC   brand as `Marke`
// MAGIC   ,count(1) as `Anzahl Biere` 
// MAGIC from products
// MAGIC where productSubCategory = 'Bier und -mischgetränke'
// MAGIC -- Es gibt Produkte ohne Markenangabe, diese werden gefiltert
// MAGIC and brand is not null
// MAGIC group by brand
// MAGIC order by count(1) desc

// COMMAND ----------

// MAGIC %sql
// MAGIC -- Ergänzend: Pie Chart mit den Top 10 Marken
// MAGIC select 
// MAGIC   brand as `Marke`
// MAGIC   ,count(1) as `Anzahl Biere` 
// MAGIC from products
// MAGIC where productSubCategory = 'Bier und -mischgetränke'
// MAGIC -- Es gibt Produkte ohne Markenangabe, diese werden gefiltert
// MAGIC and brand is not null
// MAGIC group by brand
// MAGIC order by count(1) desc
// MAGIC limit 10

// COMMAND ----------

// MAGIC %md
// MAGIC ###  10: Wie viele Produkte enthalten eine Form von Zucker als Zutat?

// COMMAND ----------

// MAGIC %sql
// MAGIC -- Frage nur sinnvoll wenn auf Lebensmittel bezogen
// MAGIC -- HINWEIS: Hier könnte noch weiter in den Unterkategorien unterschieden werden.
// MAGIC -- Z.B. ist in "Baby und Kind" auch Nahrung enthalten, die möglicherweise auch Zucker als Zutat hat
// MAGIC select distinct productCategory 
// MAGIC from products
// MAGIC where productCategory not in ('Tier', 'Küche und Haushalt', 'Drogerie und Kosmetik', 'Baby und Kind')

// COMMAND ----------

// MAGIC %sql
// MAGIC -- Annahme: Die Liste der Zuckerarten unten ist
// MAGIC -- erschöpfend. Möglicherweise gibt es noch weitere, die das Ergebnis
// MAGIC -- aber nicht wesentlich beeinflussen dürften.
// MAGIC select 
// MAGIC   productName
// MAGIC  ,lower(ingredientStatement)
// MAGIC  ,count(1) over() as `Anzahl Produkte mit Zucker`
// MAGIC  ,round( count(1) over() / (
// MAGIC     select count(1) from products where productCategory not in ('Tier', 'Küche und Haushalt', 'Drogerie und Kosmetik', 'Baby und Kind')
// MAGIC    ), 2) as `% aller Produkte`
// MAGIC from products
// MAGIC where 
// MAGIC productCategory NOT IN ('Tier', 'Küche und Haushalt', 'Drogerie und Kosmetik', 'Baby und Kind')
// MAGIC and (
// MAGIC   lower(ingredientStatement) like '%zucker%'
// MAGIC   or lower(ingredientStatement) like '%dextrose%'
// MAGIC   or lower(ingredientStatement) like '%saccharose%'
// MAGIC   or lower(ingredientStatement) like '%traubenzucker%'
// MAGIC   or lower(ingredientStatement) like '%fruktose%'
// MAGIC   or lower(ingredientStatement) like '%fructose%'
// MAGIC   or lower(ingredientStatement) like '%lactose%'
// MAGIC   or lower(ingredientStatement) like '%laktose%'
// MAGIC   or lower(ingredientStatement) like '%milchzucker%'
// MAGIC   or lower(ingredientStatement) like '%maltose%'
// MAGIC   or lower(ingredientStatement) like '%glucose%'
// MAGIC   or lower(ingredientStatement) like '%glukose%'
// MAGIC   or lower(ingredientStatement) like '%galactose%'
// MAGIC   )
// MAGIC -- Antwort: In 3529 Lebensmittelprodukten, oder in 53% aller Lebensmittelprodukte, ist eine Form von Zucker als Zutat enthalten(!)
