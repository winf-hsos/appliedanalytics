// Databricks notebook source
// MAGIC %md
// MAGIC Um dieses Notebook verwenden zu können müssen zunächst die 4 notwendigen Tabellen erstellt werden. Dazu wie folgt vorgehen:
// MAGIC <br><br>
// MAGIC 1. Download [des Datensatzes als ZIP-Datei](https://docs.google.com/uc?export=download&id=1Y2pejbWn0ZS0OU8QPAUnsUDzrJ_wF__v) (enthält 4 CSV-Dateien)
// MAGIC 2. Entpacken der ZIP-Datei
// MAGIC 3. Hochladen der 4 CSV-Dateien in das Databricks File System (DBFS)
// MAGIC   - Sicherstellen, dass ein Cluster gestartet ist. Ansonsten ein neues Cluster starten und warten, bis es hochgefahren ist
// MAGIC   - Im Menü links auf "Data" klicken, die Datenbank "default" auswählen, und dann auf das Plus-Symbol neben "Tables" klicken
// MAGIC   - Als Datasource "Upload File" auswählen (sollte bereits ausgewählt sein) und die 4 Dateien aus dem Windows-Explorer per Drag & Drop auf das Feld ziehen (alternativ "Browse" klicken und die 4 Dateien auswählen)
// MAGIC   - Nach erfolgreichem Upload (kann 1-2 Minuten dauern, je nach Verbindungsqualität) bitte die angezeigten Pfade abgleichen. Sollten diese abweichen, dann notiert euch bitte eure Pfade und ersetzt sie entsprechend in den Befehlen zum Laden der Tabellen unten:
// MAGIC     - `/FileStore/tables/script_lines.csv`
// MAGIC     - `/FileStore/tables/episodes.csv`
// MAGIC     - `/FileStore/tables/characters.csv`
// MAGIC     - `/FileStore/tables/locations.csv`
// MAGIC 4. Führt die Befehle zum Anlegen der Tabellen nacheinander aus. Klickt dazu in die jeweilige Zelle und drückt Strg + Enter, oder klickt auf den Play-Button oben rechts
// MAGIC 
// MAGIC **Hinweis**: Das Erstellen der Tabellen müsst ihr nur einmalig beim ersten Mal durchführen. Anschließend sind die Tabellen bei eurem nächsten Login bereits vorhanden.
// MAGIC   

// COMMAND ----------

// Falls Pfade bei euch abweichen bitte entsprechend ersetzen!
val script_lines = spark.read.option("header", "true")
                      .option("inferSchema", "true")
                      .option("delimiter", ";")
                      .csv("/FileStore/tables/script_lines.csv")

script_lines.write.saveAsTable("script_lines")

// COMMAND ----------

// Falls Pfade bei euch abweichen bitte entsprechend ersetzen!

val characters = spark.read.option("header", "true")
                      .option("inferSchema", "true")
                      .option("delimiter", ",")
                      .csv("/FileStore/tables/characters.csv")

characters.write.saveAsTable("characters")

// COMMAND ----------

// Falls Pfade bei euch abweichen bitte entsprechend ersetzen!

val locations = spark.read.option("header", "true")
                      .option("inferSchema", "true")
                      .option("delimiter", ",")
                      .csv("/FileStore/tables/locations.csv")

locations.write.saveAsTable("locations")

// COMMAND ----------

// Falls Pfade bei euch abweichen bitte entsprechend ersetzen!

val episodes = spark.read.option("header", "true")
                      .option("inferSchema", "true")
                      .option("delimiter", ",")
                      .csv("/FileStore/tables/episodes.csv")
episodes.write.saveAsTable("episodes")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from characters;
// MAGIC select * from locations;
// MAGIC select * from episodes;
// MAGIC select * from script_lines;

// COMMAND ----------

// MAGIC %sql
// MAGIC /* Solltet ihr aus irgendeinem Grund eine Tabelle erneut anlegen wollen
// MAGIC  * so müsst diese vorher mit dem entsprechenden Befehl unten löschen */
// MAGIC --DROP TABLE script_lines
// MAGIC --DROP TABLE characters
// MAGIC --DROP TABLE locations
// MAGIC --DROP TABLE episodes

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(1)
// MAGIC from episodes
// MAGIC 
// MAGIC -- 600

// COMMAND ----------

// MAGIC %sql
// MAGIC select season, number_in_season, title, imdb_rating
// MAGIC from episodes
// MAGIC where imdb_rating > 9

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(1)
// MAGIC from episodes
// MAGIC where year(original_air_date) = 1995

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(1) as anz
// MAGIC from script_lines
// MAGIC where normalized_text LIKE "% trump %" -- Trump in the middle of a sentence
// MAGIC or normalized_text LIKE '%trump %' -- Trump in at the beginning of a sentence
// MAGIC or normalized_text LIKE '% trump%' -- Trump at the end of a sentence

// COMMAND ----------

// MAGIC %sql
// MAGIC select c.id, c.name, sum( word_count ) as sumWords
// MAGIC from script_lines sl
// MAGIC join characters c
// MAGIC  on sl.character_id = c.id 
// MAGIC group by c.id, c.name
// MAGIC order by sumWords desc
// MAGIC limit 10 -- Show only (top) 10 results 

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(1) 
// MAGIC from script_lines
// MAGIC where normalized_text LIKE '%donut%'
// MAGIC and character_id <> 2 -- Homer Simpson

// COMMAND ----------

// MAGIC %sql
// MAGIC select sl.id, c.name, normalized_text
// MAGIC from characters c
// MAGIC join script_lines sl 
// MAGIC   on sl.character_id = c.id
// MAGIC where normalized_text like '%schadenfreude%'
// MAGIC or sl.id = 11363 -- Also get the following sentence with the actual explanation

// COMMAND ----------

// MAGIC %sql
// MAGIC -- Set of all episodes
// MAGIC select season, number_in_season, title
// MAGIC from episodes
// MAGIC except
// MAGIC (
// MAGIC -- Set of episodes in which Lisa speaks
// MAGIC select e.season, e.number_in_season, e.title
// MAGIC from episodes e
// MAGIC join script_lines sl
// MAGIC   on e.id = sl.episode_id
// MAGIC where sl.character_id = 9  
// MAGIC group by e.season, e.number_in_season, e.title
// MAGIC )
// MAGIC except
// MAGIC (
// MAGIC -- Set of episodes with no or inconsistent data 
// MAGIC select e.season, e.number_in_season, e.title
// MAGIC from episodes e
// MAGIC left join script_lines sl
// MAGIC   on e.id = sl.episode_id
// MAGIC group by e.season, e.number_in_season, e.title
// MAGIC having count(1) = 1
// MAGIC )
// MAGIC order by season, number_in_season

// COMMAND ----------

// MAGIC %sql
// MAGIC -- Another option using left join operator
// MAGIC SELECT e.id, e.season, e.number_in_season, lisa.id, e.title, nodata.id
// MAGIC FROM episodes e
// MAGIC LEFT JOIN (
// MAGIC   
// MAGIC -- All episodes where Lisa speaks
// MAGIC SELECT distinct e.id 
// MAGIC from episodes e
// MAGIC INNER JOIN script_lines sl
// MAGIC 	ON sl.episode_id = e.id
// MAGIC INNER JOIN characters c 
// MAGIC 	ON sl.character_id = c.id
// MAGIC WHERE c.name = 'Lisa Simpson'
// MAGIC ) lisa
// MAGIC   ON lisa.id = e.id
// MAGIC 
// MAGIC LEFT JOIN (
// MAGIC 
// MAGIC   -- All episodes with no or inconsistent data
// MAGIC SELECT e.id, count(1)
// MAGIC FROM episodes e
// MAGIC LEFT JOIN script_lines sl
// MAGIC 	ON sl.episode_id = e.id
// MAGIC GROUP BY e.id
// MAGIC HAVING count(1) = 1
// MAGIC ) nodata
// MAGIC   ON nodata.id = e.id
// MAGIC   
// MAGIC WHERE lisa.id is null
// MAGIC and nodata.id is null;

// COMMAND ----------

// MAGIC %sql
// MAGIC select l.name, sum(sl.word_count) as numWords
// MAGIC from locations l
// MAGIC inner join script_lines sl
// MAGIC   on l.id = sl.location_id
// MAGIC where sl.character_id = 2
// MAGIC group by l.name  
// MAGIC order by numWords desc
// MAGIC limit 10

// COMMAND ----------

// MAGIC %sql
// MAGIC select c.name, count(1) as numLines 
// MAGIC from characters c
// MAGIC inner join script_lines sl
// MAGIC on c.id = sl.character_id
// MAGIC where sl.location_id = 5
// MAGIC and sl.character_id not in (1, 2, 8, 9, 105) -- Simpsons family living in Simpson's Home
// MAGIC group by c.name
// MAGIC order by numLines desc
