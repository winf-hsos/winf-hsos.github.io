-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Network visualization with D3.js
-- MAGIC ---
-- MAGIC One quick way to identify topics in tweets is to analyze the use of keywords or hashtags systematically. A common approach is to find pairs of hashtags (or words) that are often mentioned together in the same tweets. Visualizing the result such that pairs that often appear together are drawn close to each other gives us a visual way to explore a topic map.
-- MAGIC 
-- MAGIC In this notebook, you'll learn how to quickly visualize networks using D3.js.
-- MAGIC 
-- MAGIC You can find a good introduction to the force layout with D3.js here: <a href="https://www.d3indepth.com/force-layout/" target="_blank">Force layout on d3indepth.com</a>
-- MAGIC 
-- MAGIC Find a collection of examples of what you can do with D3.js here: <a href="https://observablehq.com/@d3/gallery" target="_blank">D3.js gallery</a>

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
    from tweets
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
-- MAGIC ## 4. Create a force layout visulization with D3.js (SVG version)
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
-- MAGIC 
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

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Create a force layout visulization with D3.js (CANVAS version with zoom)
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
-- MAGIC   <html>
-- MAGIC   <head>
-- MAGIC   <meta charset="utf-8" />
-- MAGIC   <script src="https://d3js.org/d3-force.v1.min.js"></script>
-- MAGIC   <script src="https://d3js.org/d3.v4.min.js"></script>
-- MAGIC 
-- MAGIC </head>
-- MAGIC   <body>
-- MAGIC 
-- MAGIC     <div id="graphDiv"></div>
-- MAGIC 
-- MAGIC <hr/>
-- MAGIC 
-- MAGIC     <button onclick="download('png')">
-- MAGIC       Download PNG
-- MAGIC     </button>
-- MAGIC 
-- MAGIC     <button onclick="download('jpg')">
-- MAGIC       Download JPG
-- MAGIC     </button>
-- MAGIC 
-- MAGIC 
-- MAGIC  <script>
-- MAGIC       var data = { nodes: %s, links: %s };
-- MAGIC    
-- MAGIC    
-- MAGIC       // Get the maximum weight for an edge
-- MAGIC       var maxWeight = 0;
-- MAGIC       for (var i = 0; i < data.links.length; i++) {
-- MAGIC         if (maxWeight < data.links[i].weight) maxWeight = data.links[i].weight;
-- MAGIC       }
-- MAGIC 
-- MAGIC       var height = 1000;
-- MAGIC       var width = 2000;
-- MAGIC       
-- MAGIC 
-- MAGIC       // Append the canvas to the HTML document
-- MAGIC       var graphCanvas = d3
-- MAGIC         .select("#graphDiv")
-- MAGIC         .append("canvas")
-- MAGIC         .attr("width", width + "px")
-- MAGIC         .attr("height", height + "px")
-- MAGIC         .node();
-- MAGIC 
-- MAGIC       var context = graphCanvas.getContext("2d");
-- MAGIC 
-- MAGIC       var div = d3
-- MAGIC         .select("body")
-- MAGIC         .append("div")
-- MAGIC         .attr("class", "tooltip")
-- MAGIC         .style("opacity", 0);
-- MAGIC 
-- MAGIC       var simulation = d3
-- MAGIC         .forceSimulation()
-- MAGIC         .force(
-- MAGIC           "link",
-- MAGIC           d3
-- MAGIC             .forceLink()
-- MAGIC             .id(function(d) {
-- MAGIC               return d.id;
-- MAGIC             })
-- MAGIC             .distance(function(d) {
-- MAGIC               return 2 * (maxWeight - d.weight);
-- MAGIC             })
-- MAGIC             .strength(function(d) {
-- MAGIC               return 1 * (d.weight / maxWeight);;
-- MAGIC             })
-- MAGIC         )
-- MAGIC         .force("charge", d3.forceManyBody().strength(-250))
-- MAGIC         .force("center", d3.forceCenter(width / 2, height / 2))
-- MAGIC         .force("x", d3.forceX(width / 2).strength(0.01))
-- MAGIC         .force("y", d3.forceY(height / 2).strength(0.01))
-- MAGIC         .alphaTarget(0)
-- MAGIC         .alphaDecay(0.05);
-- MAGIC 
-- MAGIC       var transform = d3.zoomIdentity;
-- MAGIC 
-- MAGIC       initGraph(data);
-- MAGIC 
-- MAGIC       function initGraph(tempData) {
-- MAGIC         function zoomed() {
-- MAGIC           console.log("zooming");
-- MAGIC           transform = d3.event.transform;
-- MAGIC           simulationUpdate();
-- MAGIC         }
-- MAGIC 
-- MAGIC         d3.select(graphCanvas)
-- MAGIC           .call(
-- MAGIC             d3
-- MAGIC               .drag()
-- MAGIC               .subject(dragsubject)
-- MAGIC               .on("start", dragstarted)
-- MAGIC               .on("drag", dragged)
-- MAGIC               .on("end", dragended)
-- MAGIC           )
-- MAGIC           .call(
-- MAGIC             d3
-- MAGIC               .zoom()
-- MAGIC               .scaleExtent([1 / 10, 8])
-- MAGIC               .on("zoom", zoomed)
-- MAGIC           );
-- MAGIC 
-- MAGIC         function dragsubject() {
-- MAGIC           var i,
-- MAGIC             x = transform.invertX(d3.event.x),
-- MAGIC             y = transform.invertY(d3.event.y),
-- MAGIC             dx,
-- MAGIC             dy;
-- MAGIC           for (i = tempData.nodes.length - 1; i >= 0; --i) {
-- MAGIC             node = tempData.nodes[i];
-- MAGIC             dx = x - node.x;
-- MAGIC             dy = y - node.y;
-- MAGIC 
-- MAGIC             let radius = Math.min(30, node.size)
-- MAGIC             if (dx * dx + dy * dy < radius * radius) {
-- MAGIC               node.x = transform.applyX(node.x);
-- MAGIC               node.y = transform.applyY(node.y);
-- MAGIC 
-- MAGIC               return node;
-- MAGIC             }
-- MAGIC           }
-- MAGIC         }
-- MAGIC 
-- MAGIC         function dragstarted() {
-- MAGIC           if (!d3.event.active) simulation.alphaTarget(0.3).restart();
-- MAGIC           d3.event.subject.fx = transform.invertX(d3.event.x);
-- MAGIC           d3.event.subject.fy = transform.invertY(d3.event.y);
-- MAGIC         }
-- MAGIC 
-- MAGIC         function dragged() {
-- MAGIC           d3.event.subject.fx = transform.invertX(d3.event.x);
-- MAGIC           d3.event.subject.fy = transform.invertY(d3.event.y);
-- MAGIC         }
-- MAGIC 
-- MAGIC         function dragended() {
-- MAGIC           if (!d3.event.active) simulation.alphaTarget(0);
-- MAGIC           d3.event.subject.fx = null;
-- MAGIC           d3.event.subject.fy = null;
-- MAGIC         }
-- MAGIC 
-- MAGIC         simulation.nodes(tempData.nodes).on("tick", simulationUpdate);
-- MAGIC 
-- MAGIC         simulation.force("link").links(tempData.links);
-- MAGIC 
-- MAGIC         function render() {}
-- MAGIC 
-- MAGIC         function simulationUpdate() {
-- MAGIC           context.save();
-- MAGIC 
-- MAGIC           context.clearRect(0, 0, width, height);
-- MAGIC           context.translate(transform.x, transform.y);
-- MAGIC           context.scale(transform.k, transform.k);
-- MAGIC 
-- MAGIC           // Draw the links
-- MAGIC           tempData.links.forEach(function(d) {
-- MAGIC             context.beginPath();
-- MAGIC             context.lineWidth = Math.min(d.weight, 40);
-- MAGIC             context.strokeStyle = "rgba(0, 158, 227, .3)";
-- MAGIC             context.moveTo(d.source.x, d.source.y);
-- MAGIC             context.lineTo(d.target.x, d.target.y);
-- MAGIC             context.stroke();
-- MAGIC           });
-- MAGIC 
-- MAGIC           // Draw the nodes
-- MAGIC           tempData.nodes.forEach(function(d, i) {
-- MAGIC             context.beginPath();
-- MAGIC             context.arc(d.x, d.y, Math.min(30, d.size), 0, 2 * Math.PI, true);
-- MAGIC             context.fillStyle = "rgba(0, 158, 227, 0.8)";
-- MAGIC             context.fill();
-- MAGIC             context.fillStyle = "rgba(0, 0, 0, 1)";
-- MAGIC             context.fillText(d.id, d.x + 10, d.y);
-- MAGIC           });
-- MAGIC 
-- MAGIC           context.restore();
-- MAGIC           
-- MAGIC         }
-- MAGIC       }
-- MAGIC       
-- MAGIC     function download(type) {
-- MAGIC         var canvas = document.querySelector("canvas");
-- MAGIC 
-- MAGIC         var imgUrl;
-- MAGIC         
-- MAGIC         if (type === "png") 
-- MAGIC           imgUrl = canvas.toDataURL("image/png");
-- MAGIC         else if (type === "jpg") 
-- MAGIC           imgUrl = canvas.toDataURL("image/png");
-- MAGIC 
-- MAGIC         window.open().document.write('<img src="' + imgUrl + '" />');
-- MAGIC       }
-- MAGIC     </script>
-- MAGIC     
-- MAGIC   </body>
-- MAGIC </html>
-- MAGIC """ % (nodes, edges)
-- MAGIC 
-- MAGIC #print(html)
-- MAGIC displayHTML(html)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **NOTE**: Using a canvas element hast two advantages:<br><br>
-- MAGIC 
-- MAGIC 1. It is more performant than SVG and will allow you to visualize more nodes and edges before your browser freezes the simulation
-- MAGIC 2. You can export to JPG or PNG easily. Use the two buttons to open the canvas as images. Note that you need to right-click -> "Open in new tab" or copy the image in order to save it to disk.
