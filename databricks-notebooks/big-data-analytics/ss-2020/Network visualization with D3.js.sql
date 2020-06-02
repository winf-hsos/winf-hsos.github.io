-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Network visualization with D3.js
-- MAGIC ---
-- MAGIC One quick way to identify topics in tweets is to analyze the use of keywords or hashtags systematically. A common approach is to find pairs of hashtags (or words) that are often mentioned together in the same tweets. Visualizing the result such that pairs that often appear together are drawn close to each other gives us a visual way to explore a topic map.
-- MAGIC 
-- MAGIC In this notebook, you'll learn how to quickly visualize networks using D3.js.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Create a view for hashtag pairs

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Explode hashtag array and create a view

-- COMMAND ----------

create or replace view hashtags as
  -- Make all hashtags lower case for easier comparison
  select id
        ,lower(hashtag) as hashtag
        ,created_at
  from (
    select id, explode(hashtags) as hashtag, created_at
    from twitter_timelines
  )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create a view for hashtag pairs

-- COMMAND ----------

create or replace view hashtag_pairs as
select distinct
   id
   ,case when h1 > h2 then h2 else h1 end as h1
   ,case when h1 < h2 then h2 else h1 end as h2 
   from (
     select h1.id
           ,h1.hashtag as h1
           ,h2.hashtag as h2 
     from 
        (select id, hashtag from hashtags where year(created_at) = '2020') h1
     inner join 
        (select id, hashtag from hashtags where year(created_at) = '2020') h2
     on h1.id = h2.id
     and h1.hashtag <> h2.hashtag
  )

-- COMMAND ----------

select * from hashtag_pairs

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Export the nodes with SQL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create a view to extract the nodes

-- COMMAND ----------

create or replace view nodes as
select hashtag as `id`
      ,count(1) as `size`
from hashtags
where year(created_at) = '2020'
group by hashtag
-- Only hashtags that occured more than n times
having count(1) > 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Convert nodes to JSON

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import json
-- MAGIC df = spark.sql('select id, size from nodes')
-- MAGIC nodes = df.toJSON().map(lambda j: json.loads(j)).collect()
-- MAGIC #print(nodes)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Export the edges with SQL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create a view to extract the edges

-- COMMAND ----------

create or replace view edges as
select 
      h1 as `source`
     ,h2 as `target`
     ,count(1) as `weight`
from hashtag_pairs 
where h1 in (select id from nodes)
and h2 in (select id from nodes)
group by h1, h2
order by count(1) desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Convert edges to JSON

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import json
-- MAGIC df = spark.sql('select source, target, weight from edges')
-- MAGIC edges = df.toJSON().map(lambda j: json.loads(j)).collect()
-- MAGIC #print(edges)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Create a force layout visulization with D3.js
-- MAGIC ---
-- MAGIC You will need to play around with the parameters until you find a good fit for your data:<br><br>
-- MAGIC 
-- MAGIC 
-- MAGIC - `.distance(function(d) { return 2 * (maxWeight  - d.weight); })` - This function determines the physical length between the edges
-- MAGIC - `.strength(function(d) { return 1 * (d.weight / maxWeight); })` - This function determines elasticity of the connection - how easily does it stretch?
-- MAGIC - `.force("charge", d3.forceManyBody().strength(-250))` - This function sets the gravity (positive/negative) with which the points attract or repel each other
-- MAGIC - `.force("x", d3.forceX(width / 2).strength(.01))` - This function (for y respectively) determines the location of a center point and the strength with which each point is attracted to it

-- COMMAND ----------

-- MAGIC %python
-- MAGIC html = """
-- MAGIC <!DOCTYPE html>
-- MAGIC <meta charset="utf-8" />
-- MAGIC <style>
-- MAGIC   .links line {
-- MAGIC     stroke: #999;
-- MAGIC     stroke-opacity: 0.6;
-- MAGIC   }
-- MAGIC 
-- MAGIC   .nodes circle {
-- MAGIC     stroke: #fff;
-- MAGIC     stroke-width: 1.5px;
-- MAGIC   }
-- MAGIC 
-- MAGIC   text {
-- MAGIC     font-family: sans-serif;
-- MAGIC     font-size: 10px;
-- MAGIC   }
-- MAGIC </style>
-- MAGIC <svg width="2000" height="1000"></svg>
-- MAGIC <script src="https://d3js.org/d3.v4.min.js"></script>
-- MAGIC <script>
-- MAGIC   var svg = d3.select("svg"),
-- MAGIC     width = +svg.attr("width"),
-- MAGIC     height = +svg.attr("height");
-- MAGIC 
-- MAGIC   var color = d3.scaleOrdinal(d3.schemeCategory20);
-- MAGIC 
-- MAGIC   var data = { nodes: %s, edges: %s };
-- MAGIC 
-- MAGIC   // Get the maximum weight for an edge
-- MAGIC   var maxWeight = 0;
-- MAGIC   for(var i = 0; i < data.edges.length; i++) {
-- MAGIC     if(maxWeight < data.edges[i].weight)
-- MAGIC       maxWeight = data.edges[i].weight;
-- MAGIC   }
-- MAGIC     
-- MAGIC  var simulation = d3
-- MAGIC     .forceSimulation()
-- MAGIC     .force("link", d3.forceLink().id(function(d) { return d.id; })
-- MAGIC              .distance(function(d) { return 2 * (maxWeight  - d.weight); })      
-- MAGIC              .strength(function(d) { return 1 * (d.weight / maxWeight); })
-- MAGIC           )
-- MAGIC     .force("charge", d3.forceManyBody().strength(-250))
-- MAGIC     .force("center", d3.forceCenter(width / 2, height / 2))
-- MAGIC     .force("x", d3.forceX(width / 2).strength(.01))
-- MAGIC     .force("y", d3.forceY(height / 2).strength(.01))
-- MAGIC     //.force('collision', d3.forceCollide().radius(function(d) { return Math.min(30, d.size) }))
-- MAGIC 
-- MAGIC 
-- MAGIC   var link = svg
-- MAGIC     .append("g")
-- MAGIC     .attr("class", "links")
-- MAGIC     .selectAll("line")
-- MAGIC     .data(data.edges)
-- MAGIC     .enter()
-- MAGIC     .append("line")
-- MAGIC     .attr("stroke-width", function(d) {
-- MAGIC         return Math.min(d.weight, 40);
-- MAGIC     });
-- MAGIC 
-- MAGIC   var node = svg
-- MAGIC     .append("g")
-- MAGIC     .attr("class", "nodes")
-- MAGIC     .selectAll("g")
-- MAGIC     .data(data.nodes)
-- MAGIC     .enter()
-- MAGIC     .append("g");
-- MAGIC 
-- MAGIC   var circles = node
-- MAGIC     .append("circle")
-- MAGIC     .attr("r", function(d) {
-- MAGIC       return Math.min(30, d.size);
-- MAGIC     })
-- MAGIC     .attr("fill", function(d) {
-- MAGIC       return color(1);
-- MAGIC     })
-- MAGIC     .call(
-- MAGIC       d3
-- MAGIC         .drag()
-- MAGIC         .on("start", dragstarted)
-- MAGIC         .on("drag", dragged)
-- MAGIC         .on("end", dragended)
-- MAGIC     );
-- MAGIC 
-- MAGIC   var lables = node
-- MAGIC     .append("text")
-- MAGIC     .text(function(d) {
-- MAGIC       return d.id;
-- MAGIC     })
-- MAGIC     .attr("x", 6)
-- MAGIC     .attr("y", 3);
-- MAGIC 
-- MAGIC   node.append("title").text(function(d) {
-- MAGIC     return d.id;
-- MAGIC   });
-- MAGIC 
-- MAGIC   simulation.nodes(data.nodes).on("tick", ticked);
-- MAGIC 
-- MAGIC   simulation.force("link").links(data.edges);
-- MAGIC 
-- MAGIC   function ticked() {
-- MAGIC     link
-- MAGIC       .attr("x1", function(d) {
-- MAGIC         return d.source.x;
-- MAGIC       })
-- MAGIC       .attr("y1", function(d) {
-- MAGIC         return d.source.y;
-- MAGIC       })
-- MAGIC       .attr("x2", function(d) {
-- MAGIC         return d.target.x;
-- MAGIC       })
-- MAGIC       .attr("y2", function(d) {
-- MAGIC         return d.target.y;
-- MAGIC       });
-- MAGIC 
-- MAGIC     node.attr("transform", function(d) {
-- MAGIC       return "translate(" + d.x + "," + d.y + ")";
-- MAGIC     });
-- MAGIC   }
-- MAGIC 
-- MAGIC   function dragstarted(d) {
-- MAGIC     if (!d3.event.active) simulation.alphaTarget(0.3).restart();
-- MAGIC     d.fx = d.x;
-- MAGIC     d.fy = d.y;
-- MAGIC   }
-- MAGIC 
-- MAGIC   function dragged(d) {
-- MAGIC     d.fx = d3.event.x;
-- MAGIC     d.fy = d3.event.y;
-- MAGIC   }
-- MAGIC 
-- MAGIC   function dragended(d) {
-- MAGIC     if (!d3.event.active) simulation.alphaTarget(0);
-- MAGIC     d.fx = null;
-- MAGIC     d.fy = null;
-- MAGIC   }
-- MAGIC </script>
-- MAGIC 
-- MAGIC """ % (nodes, edges)
-- MAGIC 
-- MAGIC #print(html)
-- MAGIC displayHTML(html)
