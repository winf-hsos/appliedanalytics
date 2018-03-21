// Databricks notebook source
// MAGIC %sql
// MAGIC 
// MAGIC select id, name, (select count(1) from script_lines where character_id = c.id) as numLines
// MAGIC from characters c
// MAGIC order by numLines desc

// COMMAND ----------

// MAGIC %sql 
// MAGIC select * from episodes
// MAGIC where id in
// MAGIC ( select id from episodes where season = 2 )

// COMMAND ----------

// MAGIC %sql
// MAGIC select distinct season from episodes e
// MAGIC where exists
// MAGIC ( select * from episodes where season = e.season and imdb_rating > 9  )

// COMMAND ----------

// MAGIC %sql
// MAGIC select sub.name
// MAGIC from 
// MAGIC ( select id, name from characters where normalized_name like '%homer%' ) as sub

// COMMAND ----------

// MAGIC %sql
// MAGIC select season, title, max(imdb_rating) over (partition by season)  
// MAGIC from episodes

// COMMAND ----------

// MAGIC %sql
// MAGIC select season, number_in_season, imdb_rating, title, 
// MAGIC        rank() over (partition by season order by imdb_rating desc) as rankInSeason
// MAGIC from episodes
// MAGIC order by season, rankInSeason

// COMMAND ----------

// MAGIC %sql
// MAGIC -- All unique locations from episode 1 in season 1
// MAGIC select distinct l.name 
// MAGIC from script_lines sl
// MAGIC inner join episodes e
// MAGIC   on e.id = sl.episode_id
// MAGIC inner join locations l
// MAGIC   on l.id = sl.location_id
// MAGIC where e.season = 1 and e.number_in_season = 1
// MAGIC UNION ALL
// MAGIC -- All unique locations from episode 2 in season 1
// MAGIC select distinct l.name 
// MAGIC from script_lines sl
// MAGIC inner join episodes e
// MAGIC   on e.id = sl.episode_id
// MAGIC inner join locations l
// MAGIC   on l.id = sl.location_id
// MAGIC where e.season = 1 and e.number_in_season = 2
// MAGIC order by name

// COMMAND ----------

// MAGIC %sql
// MAGIC -- What are common locations of both episodes?
// MAGIC 
// MAGIC -- All unique locations from episode 1 in season 1
// MAGIC select distinct l.name 
// MAGIC from script_lines sl
// MAGIC inner join episodes e
// MAGIC   on e.id = sl.episode_id
// MAGIC inner join locations l
// MAGIC   on l.id = sl.location_id
// MAGIC where e.season = 1 and e.number_in_season = 1
// MAGIC INTERSECT
// MAGIC -- All unique locations from episode 2 in season 1
// MAGIC select distinct l.name 
// MAGIC from script_lines sl
// MAGIC inner join episodes e
// MAGIC   on e.id = sl.episode_id
// MAGIC inner join locations l
// MAGIC   on l.id = sl.location_id
// MAGIC where e.season = 1 and e.number_in_season = 2
// MAGIC order by name

// COMMAND ----------

// MAGIC %sql
// MAGIC -- Get all locations from the first episode that are NOT part of the seond episode
// MAGIC 
// MAGIC -- All unique locations from episode 1 in season 1
// MAGIC select distinct l.name 
// MAGIC from script_lines sl
// MAGIC inner join episodes e
// MAGIC   on e.id = sl.episode_id
// MAGIC inner join locations l
// MAGIC   on l.id = sl.location_id
// MAGIC where e.season = 1 and e.number_in_season = 1
// MAGIC EXCEPT
// MAGIC -- All unique locations from episode 2 in season 1
// MAGIC select distinct l.name 
// MAGIC from script_lines sl
// MAGIC inner join episodes e
// MAGIC   on e.id = sl.episode_id
// MAGIC inner join locations l
// MAGIC   on l.id = sl.location_id
// MAGIC where e.season = 1 and e.number_in_season = 2
// MAGIC order by name

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE OR REPLACE VIEW script_lines_enhanced
// MAGIC AS 
// MAGIC SELECT 
// MAGIC    sl.normalized_text
// MAGIC   ,e.id as episode_id, e.title, e.season, e.number_in_season 
// MAGIC   ,c.id as character_id, c.name as character_name
// MAGIC   ,l.id as location_id, l.name as location_name
// MAGIC FROM script_lines sl
// MAGIC INNER JOIN episodes e
// MAGIC   ON sl.episode_id = e.id
// MAGIC INNER JOIN characters c
// MAGIC   ON sl.character_id = c.id
// MAGIC INNER JOIN locations l
// MAGIC   ON sl.location_id = l.id

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from script_lines_enhanced
