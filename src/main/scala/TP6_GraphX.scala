//import TP6_GraphX.pathArticles
import breeze.linalg.min
import breeze.numerics.round
import org.apache.spark.SparkContext
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.graphx.{Edge, EdgeContext, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.dsl.expressions.{DslAttr, StringToAttributeConversionHelper}
//import org.apache.spark.sql.execution.datasources.noop.NoopWriteBuilder.truncate
import org.graphframes.lib.Pregel.msg
import org.apache.spark.sql.functions.{array, avg, col, collect_list, collect_set, count, explode, lit, struct, to_date, udf, when, min => sparkMin}
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession, functions}
import org.graphframes.GraphFrame
import org.graphframes.lib.AggregateMessages
import org.apache.spark.sql.functions._



object TP6_GraphX {

  // Fonction pour configurer Spark et obtenir le SparkContext
  def getConfSpark(): (SparkSession, SparkContext) = {
    val spark = SparkSession.builder()
      .appName("SparkApp6_GraphX")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    // Récupération du SparkContext depuis SparkSession
    val sc = spark.sparkContext
    (spark, sc)
  }

  /*
  case class Person(name: String, age: Int)
  def create_Graph(sc: SparkContext): Graph[Person, String] = {
    val vertices: RDD[(Long, Person)] = sc.parallelize(
      Array(
        (1L, Person("Homer", 39)),
        (2L, Person("Marge", 39)),
        (3L, Person("Bart", 12)),
        (4L, Person("Milhouse", 12))
      )
    )
    val edges: RDD[Edge[String]] = sc.parallelize(
      Array(
        Edge(4L, 3L, "friend"),
        Edge(3L, 1L, "father"),
        Edge(3L, 2L, "mother"),
        Edge(1L, 2L, "marriedTo")
      )
    )
    val graph = Graph(vertices, edges)
    graph
  }

  case class Relationship(relation: String)
  case class PersonExt(name: String, age: Int, children: Int = 0, friends: Int = 0, married: Boolean = false)

  def transform_Edges(graph: Graph[Person, String]): Graph[Person, Relationship] = {
    val newGraph = graph.mapEdges((partId, iter) => iter.map(edge =>
      Relationship(edge.attr)))
    return newGraph
  }

  def transform_Vertices(graph: Graph[Person, Relationship]): Graph[PersonExt,
    Relationship] = {
    val graph2 = graph.mapVertices((vid, person) => PersonExt(person.name, person.age))
    return graph2
  }

  def update_Graph(graph: Graph[PersonExt, Relationship]): VertexRDD[(Int, Int, Boolean)] = {
    val updated_Vertices = graph.aggregateMessages(
      (ctx: EdgeContext[PersonExt, Relationship, Tuple3[Int, Int, Boolean]]) => {
        if (ctx.attr.relation == "marriedTo") {
          ctx.sendToSrc((0, 0, true));
          ctx.sendToDst((0, 0, true));
        }
        else if (ctx.attr.relation == "mother" || ctx.attr.relation == "father") {
          ctx.sendToDst((1, 0, false));
        }
        else if (ctx.attr.relation == "friend") {
          ctx.sendToDst((0, 1, false));
          ctx.sendToSrc((0, 1, false));
        }
      },
      (msg1: Tuple3[Int, Int, Boolean], msg2: Tuple3[Int, Int, Boolean]) =>
        (msg1._1 + msg2._1, msg1._2 + msg2._2, msg1._3 || msg2._3)
    )
    return updated_Vertices
  }

  def join_Graph(graph: Graph[PersonExt, Relationship], updatedV: VertexRDD[(Int,
    Int, Boolean)]): Graph[PersonExt, Relationship] = {
    val joinedGraph = graph.outerJoinVertices(updatedV)(
      (vid, origPerson, optMsg) => {
        optMsg match {
          case Some(msg) =>
            PersonExt(origPerson.name, origPerson.age, msg._1, msg._2, msg._3)
          case None => origPerson
        }
      }
    )
    return joinedGraph
  }

  def select_subgraph(graph: Graph[PersonExt, Relationship]): Graph[PersonExt, Relationship] = {
    // Les sommets ayant des enfants sont sélectionnés.
    // Les arêtes associées aux sommets non sélectionnés sont automatiquement supprimées.
    val parents = graph.subgraph(
      epred = _ => true, // Tous les types d'arêtes sont conservés.
      vpred = (vertexId, person) => person.children > 0 // Conserver uniquement les parents.
    )
    parents
  }

  val pathArticles = "C:\\Users\\nitie\\Documents\\MON BUREAU PERSONNEL\\UNIVERSITE EUROMED FES\\EIDIA-Big Data-3A\\Méga Donnée II\\Projets TP\\TP6_SparkGraphX_MD2\\datasets\\articles.tsv"
  val pathLinks = "C:\\Users\\nitie\\Documents\\MON BUREAU PERSONNEL\\UNIVERSITE EUROMED FES\\EIDIA-Big Data-3A\\Méga Donnée II\\Projets TP\\TP6_SparkGraphX_MD2\\datasets\\links.tsv"
  def atelier_graphX_creation(sc: SparkContext): Graph[Int, Int] = {
    val articles = sc.textFile(pathArticles).filter(line => line.trim() != " " && !line.startsWith("#")).zipWithIndex().cache()
    val links = sc.textFile(pathLinks).filter(line => line.trim() != "" && !line.startsWith("#"))
    //Analysez chaque ligne de liens pour obtenir les noms d'articles, puis
    //Remplacez chaque nom par l'ID d'article en joignant les noms avec les articles
    val linkIndexes = links.map(x => {
      val spl = x.split("\t"); (spl(0), spl(1))}).join(articles).map(x => x._2).join(articles).map(x => x._2)
    //Le RDD résultant contient des tuples avec des ID d'article source et de destination.
      //l'utiliser pour construire un objet Graph en utilisant une variante des tuples edges
    val wikigraph = Graph.fromEdgeTuples(linkIndexes, 0)
    //valider le nombre d'articles et le nombre de vertex (certains articles manquent dans le fichier de liens )
    println(wikigraph.vertices.count())
    println(articles.count())
    return wikigraph
  }

  def atelier_graphX_shortpath(sc: SparkContext, graph: Graph[Int, Int]): Unit = {
    val articles = sc.textFile(pathArticles).filter(line => line.trim() != " " && !line.startsWith("#")).zipWithIndex().cache()
    articles.filter(x => x._1 == "Rainbow" || x._1 == "14th_century").collect().foreach(println)
    val shortest = ShortestPaths.run(graph, Seq(10))
    shortest.vertices.filter(x => x._1 == 3425).collect.foreach(println)
  }

  def atelier_graphX_pageRank(sc: SparkContext, graph: Graph[Int, Int]): Unit = {
    val articles = sc.textFile(pathArticles).filter(line => line.trim() != "" && !line.startsWith("#"))
      .zipWithIndex().cache()
    val page_ranked = graph.pageRank(0.001)
    //les 10 pages les mieux classées du sous-ensemble Wikispeedia des pages Wikipédia
    val ordering_fct = new Ordering[Tuple2[VertexId, Double]] {
      def compare(x: Tuple2[VertexId, Double], y: Tuple2[VertexId, Double]): Int =
        x._2.compareTo(y._2)
    }
    val top10_pages = page_ranked.vertices.top(10)(ordering_fct)
    val result = sc.parallelize(top10_pages).join(articles.map(_.swap)).collect()
      .sortWith((x, y) => x._2._1 > y._2._1)
    result.foreach(println)
  }

  def atelier_graphX_CC(sc: SparkContext, graph: Graph[Int, Int]): Unit = {
    val articles = sc.textFile(pathArticles).filter(line => line.trim() != " " && !line.startsWith("#"))
      .zipWithIndex().cache()
    val pagesCC = graph.connectedComponents()
    //les composants connectés et le nom du vertex avec l'id le plus bas
    val result = pagesCC.vertices.map(x => (x._2, x._2))
    result.distinct().join(articles.map(_.swap)).collect.foreach(println)
    //Combien de pages y a-t-il dans chaque composants?
    result.countByKey().foreach(println)
  }

  def atelier_graphX_SCC(sc: SparkContext, graph: Graph[Int, Int]): Unit = {
    // Chargement des articles et association avec leurs indices
    val articles = sc.textFile(pathArticles)
      .filter(line => line.trim.nonEmpty && !line.startsWith("#")).zipWithIndex().map { case (line, index) => (index.toLong, line) }.cache()

    // Calcul des composantes fortement connexes (SCC)
    val pagesSCC = graph.stronglyConnectedComponents(100)

    // Comptage des SCC distinctes
    val SCC_count = pagesSCC.vertices.map { case (_, sccId) => sccId }.distinct().count()
    println(s"Nombre de composants fortement connectés : $SCC_count")

    // Identification des SCC les plus grandes
    val largest = pagesSCC.vertices
      .map { case (_, sccId) => (sccId, 1) }
      .reduceByKey(_ + _).filter { case (_, count) => count > 1 }.sortBy(_._2, ascending = false).collect()

    println("Les plus grands composants fortement connectés :")
    largest.foreach { case (sccId, count) =>
      println(s"SCC ID: $sccId, Nombre de sommets: $count")
    }

    // Examen des pages appartenant au deuxième plus grand SCC
    if (largest.length > 1) {
      val secondLargestSCCId = largest(1)._1
      val secondLargestPages = pagesSCC.vertices
        .filter { case (_, sccId) => sccId == secondLargestSCCId }.join(articles).collect()

      println(s"Pages appartenant au 2ème plus grand SCC (ID: $secondLargestSCCId) :")
      secondLargestPages.foreach { case (_, (vertexId, pageName)) =>
        println(s"Sommet ID: $vertexId, Nom de la page: $pageName")
      }
    } else {
      println("Il n'y a pas de 2ème composant fortement connecté.")
    }
  }

  def create_grapheFrame(spark: SparkSession): GraphFrame = {
    val vertices_DF = spark.createDataFrame(List(("a", "Alice", 34), ("b", "Bob", 36),
      ("c", "Charlie", 30), ("d", "David", 29), ("e", "Esther", 32), ("f", "Fanny", 36),
      ("g", "Gabby", 60))).toDF("id", "name", "age")
    val edges_DF = spark.createDataFrame(List(("a", "b", "friend"),
      ("b", "c", "follow"), ("c", "b", "follow"), ("f", "c", "follow"),
      ("e", "f", "follow"), ("e", "d", "friend"), ("d", "a", "friend"),
      ("a", "e", "friend"))).toDF("src", "dst", "relationship")
    val graph = GraphFrame(vertices_DF, edges_DF)
    return graph
  }

  def basic_queries(graph: GraphFrame): Unit = {
    //affichage de vertices et edges
    graph.vertices.show()
    graph.edges.show()
    //Méthode intégrée
    graph.degrees.show()
    graph.inDegrees.show()
    graph.outDegrees.show()
    //Méthode intégrée avec des requestes DataFrame
    graph.inDegrees.filter("inDegree >= 2").show()
    //l’âge de la personne la plus jeune dans le graphe
    graph.vertices.groupBy().min("age").show()
    //Combien d'utilisateurs de notre réseau social ont un âge > 35
    println(graph.vertices.filter("age > 35").count())
    //comptez le nombre de relations « follow » dans le graph
    println(graph.edges.filter("relationship = 'follow'").count())
  }

  def find_motif(graph: GraphFrame): Unit = {
    // Recherche des paires de sommets avec des arêtes dans les deux directions entre eux.
    val motifs = graph.find("(a)-[e]->(b); (b)-[e2]->(a)")
    motifs.show()
    motifs.printSchema()
    //Retrouver toutes les relations réciproques dans lesquelles une personne a plus de 30 ans
    val filtered = motifs.filter("b.age > 30")
    filtered.show()
    //2ème exemple
    val motifs2 = graph.find("(1)-[edge]->(2)")
    motifs2.show()
    motifs2.printSchema()
    motifs2.select("edge.relationship").show()
  }

  // Définir la méthode de mise à jour de l'état en fonction de l'élément suivant du motif.
  def sumFriends(cnt: Column, relationship: Column): Column = {
    return when(relationship === "friend", cnt + 1).otherwise(cnt)
  }
  def stateful_queries(graph: GraphFrame): Unit = {
    // Trouver des chaînes de 4 sommets
    val chain = graph.find("(a)-[ab]->(b); (b)-[bc]->(c); (c)-[cd]->(d)")
    // Requête sur séquence, avec état (cnt)
    // Utilisez l'opération de séquence pour appliquer la méthode de màj à la séquence d 'éléments du motif.
    // Dans ce cas, les éléments sont les 3 arêtes.
    val condition = Seq("ab", "bc", "cd").foldLeft(lit(0))((cnt, e) => sumFriends(cnt, col(e)("relationship")))
    // (c) Appliquer le filtre à DataFrame.
    val chainWith2Friends = chain.where(condition >= 2)
    chainWith2Friends.show()
  }

  def sub_graph(graph: GraphFrame): Unit = {
    val subgraph = graph.filterEdges("relationship = 'friend'")
      .filterVertices("age > 30")
      .dropIsolatedVertices()
    subgraph.vertices.show()
    subgraph.edges.show()
  }

  def Complex_triplet_filters(graph: GraphFrame): Unit = {
    val filtered = graph.find("(a)-[e]->(b)").filter("e.relationship ='follow'")
      .filter(" a.age < b.age")
    val edges_extrait = filtered.select("e.src", "e.dst", "e.relationship") //val e2 = filtered.select("e.*")
    // Construire le sous-graphe
    val graph_filtered = GraphFrame(graph.vertices, edges_extrait)
    graph_filtered.vertices.show()
    graph_filtered.edges.show()
  }

  val pathTransportnodes = "C:\\Users\\nitie\\Documents\\MON BUREAU PERSONNEL\\UNIVERSITE EUROMED FES\\EIDIA-Big Data-3A\\Méga Donnée II\\Projets TP\\TP6_SparkGraphX_MD2\\datasets\\transport-nodes.csv"
  val pathTransportrelationships = "C:\\Users\\nitie\\Documents\\MON BUREAU PERSONNEL\\UNIVERSITE EUROMED FES\\EIDIA-Big Data-3A\\Méga Donnée II\\Projets TP\\TP6_SparkGraphX_MD2\\datasets\\transport-relationships.csv"
  def create_transport_graph(spark: SparkSession): GraphFrame = {
    import spark.implicits._
    val nodes_schema = StructType(Array(StructField("id", StringType, true),
      StructField("latitude", FloatType, true), StructField("longitude", FloatType,
        true), StructField("population", IntegerType, true)))
    val nodes_DF = spark.read.schema(nodes_schema).option("header", "true")
      .csv(pathTransportnodes)
    val relations_DF = spark.read.option("header", "true")
      .csv(pathTransportrelationships)
    val reversed_rels = relations_DF.withColumn("newSrc", col("dst"))
      .withColumn("newDst", col("src")).drop("dst", "src")
      .withColumnRenamed("newSrc", "src").withColumnRenamed("newDst", "dst")
      .select("src", "dst", "relationship", "cost")
    val relationships_DF = relations_DF.union(reversed_rels)
    return GraphFrame(nodes_DF, relationships_DF)
  }

  def BFS_Application(spark: SparkSession, graph: GraphFrame): Unit = {
    import spark.implicits._
    //Vérifier premièrement les villes qui respectent la condition
    graph.vertices.filter("population > 100000 and population < 300000")
      .sort("population").show()
    val result = graph.bfs
      .fromExpr("id='Den Haag'")
      .toExpr("population > 100000 and population < 300000 and id <> 'Den Haag'").run()
    //Le résultat contient des colonnes qui décrivent les nœuds et les relations entre les deux villes
    //Les colonnes commençant par e représentent les relations (arêtes)
    //Les colonnes commençant par v représentent les nœuds (sommets)
    println(result.columns.toList)
    //Filtrer toutes les colonnes commençant par e du DataFrame résultant (Garder que les noeuds)
    val columns = result.columns.filter(c => !c.startsWith("e")).toList
    println(columns)
    result.select(columns.map(m => col(m)): _*).show()
  }

  def shortest_path_graph_Transport(graph: GraphFrame, landmark: String):
  DataFrame = {
    val result = graph.shortestPaths.landmarks(Seq(landmark)).run()
    return result
  }

  // Fonction pour ajouter un nœud au chemin existant
  def add_path(path: List[String], id: String): List[String] = {
    id :: path
  }
  def Weighted_shortestPath(spark: SparkSession, graph: GraphFrame, origine: String, dest: String): DataFrame = {
    import spark.implicits._
    val AM = AggregateMessages
    val column_name = "cost" // Colonne représentant les poids du graphe

    // Validation de l'existence de la destination
    if (graph.vertices.filter($"id" === dest).count() == 0) {
      println("Destination inconnue")
      return spark.emptyDataFrame
    }

    // Initialisation des colonnes
    val vertices = graph.vertices
      .withColumn("visited", lit(false))
      .withColumn("distance", when($"id" === origine, 0.0).otherwise(Double.PositiveInfinity))
      .withColumn("path", array()).cache()

    val cached_vertices = AM.getCachedDataFrame(vertices)
    var newGraph = GraphFrame(cached_vertices, graph.edges)

    // Définition de l'UDF pour ajouter un chemin
    val add_path_udf = udf((path: Seq[String], id: String) => add_path(path.toList, id).toArray)

    while (newGraph.vertices.filter($"visited" === false).count() != 0) {
      // Sélection du nœud courant
      val current_node_id = newGraph.vertices.filter($"visited" === false).sort($"distance").first().getAs[String]("id")

      // Préparation des messages pour diffusion
      val msg_distance = AM.edge(column_name) + AM.src("distance")
      val msg_path = add_path_udf(AM.src("path"), AM.src("id"))
      val msg_for_dst = when(AM.src("id") === current_node_id, struct(msg_distance.alias("col1"), msg_path.alias("col2")))

      // Diffusion des messages et agrégation
      val new_distances = newGraph.aggregateMessages
        .sendToDst(msg_for_dst)
        .agg(sparkMin(AM.msg).alias("aggMess"))

      // Mise à jour des colonnes
      val new_visited_col = when($"visited" || ($"id" === current_node_id), true).otherwise(false)
      val new_distance_col = when(new_distances("aggMess").isNotNull && (new_distances("aggMess")("col1") < $"distance"),
        new_distances("aggMess")("col1")).otherwise($"distance")
      val new_path_col = when(new_distances("aggMess").isNotNull && (new_distances("aggMess")("col1") < $"distance"),
        new_distances("aggMess")("col2").cast("array<string>")).otherwise($"path")

      // Mise à jour des vertices
      val new_vertices = newGraph.vertices
        .join(new_distances, Seq("id"), "left_outer")
        .withColumn("visited", new_visited_col)
        .withColumn("newDistance", new_distance_col)
        .withColumn("newPath", new_path_col)
        .drop("aggMess", "distance", "path")
        .withColumnRenamed("newDistance", "distance")
        .withColumnRenamed("newPath", "path")

      val cached_new_vertices = AM.getCachedDataFrame(new_vertices)
      newGraph = GraphFrame(cached_new_vertices, newGraph.edges)

      // Vérification de la condition de sortie
      if (newGraph.vertices.filter($"id" === dest).select("visited").first().getBoolean(0)) {
        return newGraph.vertices
          .filter($"id" === dest)
          .withColumn("newPath", add_path_udf($"path", $"id"))
          .drop("visited", "path")
          .withColumnRenamed("newPath", "path")
      }
    }

    // Retourne un DataFrame vide si aucune solution n'est trouvée
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], graph.vertices.schema)
      .withColumn("path", array())
      .withColumn("distance", lit(0))
  }

  val pathSocialnodes = "C:\\Users\\nitie\\Documents\\MON BUREAU PERSONNEL\\UNIVERSITE EUROMED FES\\EIDIA-Big Data-3A\\Méga Donnée II\\Projets TP\\TP6_SparkGraphX_MD2\\datasets\\social-nodes.csv"
  val pathSocialrelationship = "C:\\Users\\nitie\\Documents\\MON BUREAU PERSONNEL\\UNIVERSITE EUROMED FES\\EIDIA-Big Data-3A\\Méga Donnée II\\Projets TP\\TP6_SparkGraphX_MD2\\datasets\\social-relationships.csv"
  def create_twitter_graph(spark: SparkSession): GraphFrame = {
    val nodes = spark.read.option("header", "true").csv(pathSocialnodes)
    val edges = spark.read.option("header", "true").csv(pathSocialrelationship)
    val graph = GraphFrame(nodes, edges)
    return graph
  }

  def degreeCentrality(graph: GraphFrame): DataFrame = {
    val total_degree = graph.degrees
    val in_degree = graph.inDegrees.withColumnRenamed("id", "join_id")
    val out_degree = graph.outDegrees.withColumnRenamed("id", "join_id")
    val JoinExpression1 = total_degree.col("id") ===
      in_degree.col("join_id")
    val JoinExpression2 = total_degree.col("id") ===
      out_degree.col("join_id")
    val result = total_degree.join(in_degree, JoinExpression1, "left_outer")
      .drop("join_id")
      .join(out_degree, JoinExpression2, "left_outer")
      .drop("join_id")
      .na.fill(0).sort("inDegree")
    return result
  }

  def page_rank(spark: SparkSession, graph: GraphFrame): Unit = {
    import spark.implicits._
    // val results =
    graph.pageRank.maxIter(20).resetProbability(0.15).run()
    // results.vertices.orderBy('pagerank.desc).show()
    val results = graph.pageRank.resetProbability(0.15).tol(0.01).run()
    results.vertices.sort('pagerank.desc).show()
  }

  def PPR_twitter(spark: SparkSession, graph: GraphFrame): Unit = {
    import spark.implicits._
    val user = "Doug"
    val results =
      graph.pageRank.maxIter(20).resetProbability(0.15).sourceId(user).run()
    val people_to_follow = results.vertices.sort('pagerank.desc)
    val already_follows = graph.edges.filter('src === user).select("dst")
    already_follows.createOrReplaceTempView("persons")
    results.vertices.where('id =!= user).createOrReplaceTempView("results")
    val res = spark.sql(
      """select * from results where id not in (select dst from persons)""")
    res.show()
  }

  val pathSwnodes = "C:\\Users\\nitie\\Documents\\MON BUREAU PERSONNEL\\UNIVERSITE EUROMED FES\\EIDIA-Big Data-3A\\Méga Donnée II\\Projets TP\\TP6_SparkGraphX_MD2\\datasets\\sw-nodes.csv"
  val swrelationships = "C:\\Users\\nitie\\Documents\\MON BUREAU PERSONNEL\\UNIVERSITE EUROMED FES\\EIDIA-Big Data-3A\\Méga Donnée II\\Projets TP\\TP6_SparkGraphX_MD2\\datasets\\sw-relationships.csv"
  def create_dependence_graph(spark: SparkSession): GraphFrame = {
    val nodes = spark.read.option("header", "true").csv(pathSwnodes)
    val edges = spark.read.option("header", "true").csv(swrelationships)
    val graph = GraphFrame(nodes, edges)
    return graph
  }

  def triangle_count(spark: SparkSession, graph: GraphFrame): Unit = {
    import spark.implicits._
    val result = graph.triangleCount.run()
    result.sort('count.desc).filter('count > 0).show()
  }

  val pathOutput = "C:\\Users\\nitie\\Documents\\MON BUREAU PERSONNEL\\UNIVERSITE EUROMED FES\\EIDIA-Big Data-3A\\Méga Donnée II\\Projets TP\\TP6_SparkGraphX_MD2\\Graphframe"
  def connected_components_DG(spark: SparkSession, graph: GraphFrame): Unit = {
    spark.sparkContext.setCheckpointDir(pathOutput)
    val result = graph.connectedComponents.run()
    result.sort("component").groupBy("component").agg(collect_list("id")
      .alias("libraries")).show(false)
  }

  def SSC_DG(spark: SparkSession, graph: GraphFrame): Unit = {
    spark.sparkContext.setCheckpointDir(pathOutput)
    val result = graph.stronglyConnectedComponents.maxIter(10).run()

    result.sort("component").groupBy("component").agg(collect_list("id").alias
    ("libraries")).show(false)
  }

  def LabelPropagation(spark: SparkSession, graph: GraphFrame): Unit = {
    // Configurer un répertoire de checkpoint si nécessaire
    spark.sparkContext.setCheckpointDir(pathOutput)

    // Exécuter l'algorithme de propagation de labels
    val result = graph.labelPropagation.maxIter(10).run()

    // Regrouper les résultats par label et afficher les communautés
    result.sort("label")
      .groupBy("label")
      .agg(collect_list("id").alias("communities"))
      .show(false)
  }

  val pathAirports = "C:\\Users\\nitie\\Documents\\MON BUREAU PERSONNEL\\UNIVERSITE EUROMED FES\\EIDIA-Big Data-3A\\Méga Donnée II\\Projets TP\\TP6_SparkGraphX_MD2\\datasets\\airports.csv"
  val pathRelationships = "C:\\Users\\nitie\\Documents\\MON BUREAU PERSONNEL\\UNIVERSITE EUROMED FES\\EIDIA-Big Data-3A\\Méga Donnée II\\Projets TP\\TP6_SparkGraphX_MD2\\datasets\\relationships.csv"
  val pathAirlines = "C:\\Users\\nitie\\Documents\\MON BUREAU PERSONNEL\\UNIVERSITE EUROMED FES\\EIDIA-Big Data-3A\\Méga Donnée II\\Projets TP\\TP6_SparkGraphX_MD2\\datasets\\airlines.csv"

  def create_Airlines_Graphes(spark: SparkSession): GraphFrame = {
    val airports = spark.read.option("header", "false").csv(pathAirports)
    val cleaned_nodes = airports.select("_c1", "_c3", "_c4", "_c6", "_c7")
      .filter("_c3 = 'United States'")
      .withColumnRenamed("_c1", "name")
      .withColumnRenamed("_c4", "id")
      .withColumnRenamed("_c6", "latitude")
      .withColumnRenamed("_c7", "longitude")
      .drop("_c3").filter("id != '\\N'")
    //cleaned_nodes.show(10)
    val relationships = spark.read.option("header", "true").csv(pathRelationships)
    val cleaned_relationships = relationships.select("ORIGIN", "DEST", "FL_DATE",
      "DEP_DELAY", "ARR_DELAY", "DISTANCE", "TAIL_NUM", "FL_NUM", "CRS_DEP_TIME",
      "CRS_ARR_TIME", "UNIQUE_CARRIER").withColumnRenamed("ORIGIN",
      "src").withColumnRenamed("DEST", "dst").withColumnRenamed("DEP_DELAY", "deptDelay")
      .withColumnRenamed("ARR_DELAY", "arrDelay").withColumnRenamed("TAIL_NUM",
      "tailNumber")
      .withColumnRenamed("FL_NUM", "flightNumber")
      .withColumnRenamed("FL_DATE", "date")
      .withColumnRenamed("CRS_DEP_TIME", "time")
      .withColumnRenamed("CRS_ARR_TIME", "arrivalTime")
      .withColumnRenamed("DISTANCE", "distance")
      .withColumnRenamed("UNIQUE_CARRIER", "airline")
      .withColumn("deptDelay", col("deptDelay").cast(FloatType))
      .withColumn("arrDelay", col("arrDelay").cast(FloatType))
      .withColumn("time", col("time").cast(IntegerType))
      .withColumn("arrivalTime", col("arrivalTime").cast(IntegerType))
    //cleaned_relationships.show(10)
    return GraphFrame(cleaned_nodes, cleaned_relationships)
  }

  def airlines_reference_DF(spark: SparkSession): DataFrame = {
    import spark.implicits._
    val airlines_reference = spark.read
      .csv(pathAirlines)
      .select("_c1", "_c3").withColumnRenamed("_c1", "name").withColumnRenamed("_c3",
      "code").filter('code =!= "null" && 'code =!= "-")
    //airlines_reference.show(10)
    return airlines_reference
  }

  def popular_departing_airports(spark: SparkSession, graph: GraphFrame): Unit = {
    import spark.implicits._
    val airports_degree = graph.outDegrees.withColumnRenamed("id", "oId")
    val result = airports_degree.join(graph.vertices,
      airports_degree.col("oId") === graph.vertices.col("id"))
      .sort('outDegree.desc).select("id", "name", "outDegree")
    result.show(10, false)
  }

  def delay_from_ORD(spark: SparkSession, graph: GraphFrame): Unit = {
    import spark.implicits._
    import org.apache.spark.sql.functions._ // Assurez-vous que cette ligne est présente

    // Filtrer les vols ayant un retard en départ depuis ORD
    val delayed_flights = graph.edges
      .filter($"src" === "ORD" && $"deptDelay" > 0)
      .groupBy("dst")
      .agg(
        avg($"deptDelay").alias("avgDelay"),
        count($"deptDelay").alias("numDelays") // Utilisation correcte de count
      )
      // Appliquer la fonction round de Spark SQL pour arrondir avgDelay à 2 décimales
      .withColumn("averageDelay", org.apache.spark.sql.functions.round($"avgDelay", 2)) // Préciser round de Spark SQL
      .withColumnRenamed("dst", "destination")

    // Joindre avec les sommets pour inclure les informations des destinations
    val result = delayed_flights
      .join(graph.vertices, delayed_flights("destination") === graph.vertices("id"))
      .sort($"averageDelay".desc)
      .select(
        $"destination".alias("Destination ID"),
        $"name".alias("Destination Name"),
        $"averageDelay".alias("Average Delay (min)"),
        $"numDelays".alias("Number of Delays")
      )

    // Afficher les 10 premiers résultats
    result.show(10, false)
  }

  def delay_details_ORD_CKB(spark: SparkSession, graph: GraphFrame): Unit = {
    import spark.implicits._
    val vols_ord_to_ckb = graph.bfs.fromExpr("id = 'ORD'").toExpr("id ='CKB'").run()
    //vols_ord_to_ckb.show(20)
    //vols_ord_to_ckb.printSchema()
    vols_ord_to_ckb.select(col("e0.date"), col("e0.time"), col("e0.flightNumber"),
      col("e0.deptDelay")).sort("date").show(5)
  }

  def bad_day_SFO(spark: SparkSession, graph: GraphFrame): Unit = {
    import spark.implicits._
    val motifs = graph.find("(a)-[ab]->(b); (b)-[bc]->(c)")
      .filter("b.id == 'SFO'")
      .filter("ab.date == '2018-05-11' and bc.date == '2018-05-11'")
      .filter("ab.arrDelay > 30 or bc.deptDelay > 30")
      .filter("ab.flightNumber == bc.flightNumber")
      .filter("ab.airline == bc.airline")
      .filter("ab.time < bc.time")
    val result = motifs.withColumn("diff",
      motifs.col("bc").getItem("deptDelay") - motifs.col("ab").getItem("arrDelay"))
      .select("ab", "bc", "diff")
      .sort('diff.desc)
    result.select(col("ab.src").alias("a1"), col("ab.time").alias("a1DeptTime"),
      col("ab.arrDelay"), col("ab.dst").alias("a2"),
      col("bc.time").alias("a2DeptTime"), col("bc.deptDelay"),
      col("bc.dst").alias("a3"), col("ab.airline"), col("ab.flightNumber"), col("diff"))
      .show(false)
  }

  def interconnected_airlines(spark: SparkSession, graph: GraphFrame): DataFrame = {
    import spark.implicits._
    val airlines = graph.edges.groupBy("airline").agg(functions.count("airline").alias("flights")).sort('flights.desc)
    val airlines_reference = airlines_reference_DF(spark)
    val full_name_airlines = airlines_reference.join(airlines, airlines.col("airline") === airlines_reference.col("code"))
      .select("code", "name", "flights")
    full_name_airlines.show(false)
    return full_name_airlines
  }

  def find_large_scc_by_airline(spark: SparkSession, graph: GraphFrame, airL: String): Row = {
    import spark.implicits._
    //Créer un sous-graphe contenant uniquement les vols de la compagnie aérienne fournie
    val airline_relationships = graph.edges.filter('airline === airL)
    val airline_graph = GraphFrame(graph.vertices, airline_relationships)
    //Calculer les composants fortement connectés
    val scc = airline_graph.stronglyConnectedComponents.maxIter(10).run()
    //Trouver et renvoyer la taille du plus gros composant
    val result =
      scc.groupBy("component").agg(count("id").alias("size")).sort('size.desc).first()
    return result
  }

  import scala.collection.mutable.ListBuffer

  def find_scc(spark: SparkSession, graph: GraphFrame, airLines: DataFrame): Unit = {
    import spark.implicits._

    // Collecte des codes des compagnies aériennes
    val lines = airLines.select("code").as[String].collect()
    // Liste pour stocker les résultats des SCC par compagnie aérienne
    val air_ssc = new ListBuffer[(String, Long)]()
    // Calcul des plus grands composants fortement connectés pour chaque compagnie aérienne
    lines.foreach { line =>
      val scc = find_large_scc_by_airline(spark, graph, line)
      val sccCount = scc.getLong(1) // Supposons que la fonction retourne un Row avec le compte SCC en 2e colonne
      air_ssc += ((line, sccCount))
    }
    // Créer un DataFrame à partir de la liste air_ssc
    val airlineSccDF = air_ssc.toSeq.toDF("codeAirline", "sccCount")
    // Joindre le DataFrame SCC avec le DataFrame des compagnies aériennes
    val airline_reach = airlineSccDF.join(airLines, airLines("code") === airlineSccDF("codeAirline"))
      .select("code", "name", "flights", "sccCount")
      .sort($"sccCount".desc)

    // Afficher les résultats
    airline_reach.show(false)
  }

  def airports_community(spark: SparkSession, graph: GraphFrame): DataFrame = {
    import spark.implicits._
    // Filtrer les arêtes pour obtenir les relations d'une compagnie aérienne spécifique (par exemple 'DL')
    val airline_relationships = graph.edges.filter("airline == 'DL'")
    // Créer un sous-graphe avec les relations de la compagnie aérienne
    val airline_graph = GraphFrame(graph.vertices, airline_relationships)
    // Appliquer l'algorithme de Propagation des labels pour détecter les communautés
    val clusters = airline_graph.labelPropagation.maxIter(10).run()
    // Agréger les communautés en comptant les aéroports et les classer
    val result = clusters
      .sort("label") // Trier par label
      .groupBy("label") // Grouper par label de communauté
      .agg(
        collect_set("id").alias("airports"), // Collecter les aéroports de la même communauté
        count("id").alias("count") // Compter le nombre d'aéroports
      )
      .where(functions.size('airports) > 1) // Garder les communautés avec plus d'un aéroport
      .sort('count.desc) // Trier par taille de communauté
    //result.show(10, false)
    return result
  }

  def airports_clusters_detail(spark: SparkSession, graph: GraphFrame, clusters: DataFrame): Unit = {
    import spark.implicits._
    // Obtenir les degrés de chaque aéroport (nombre de vols pour chaque aéroport)
    val all_flights = graph.degrees.withColumnRenamed("id", "degreeId")
    // Aplatir les clusters pour traiter chaque aéroport individuellement
    val flattenedClusters = clusters
      .select($"label", explode($"airports").alias("airportId")) // Applatir la liste d'aéroports
      .join(all_flights, all_flights.col("degreeId") === $"airportId") // Faire la jointure sur airportId
      .sort($"degree".desc)
      .select("airportId", "degree")
    flattenedClusters.show(false)
  } */







  // I.2. ATP World Tour Tennis Data Analysis (2020-2022)
  import org.apache.spark.sql.{DataFrame, SparkSession}
  import org.apache.spark.sql.functions._
  import org.graphframes.GraphFrame

  def createDataFrames(spark: SparkSession,
                       playersPath: String,
                       matchesPath: String): (DataFrame, DataFrame) = {

    // Charger les fichiers CSV des joueurs
    val playersDF = spark.read.option("header", "true").option("inferSchema", "true").csv(playersPath)

    // Charger les fichiers CSV des matches
    val matchesDF = spark.read.option("header", "true").option("inferSchema", "true").csv(matchesPath)

    // Sélectionner et transformer les colonnes importantes pour les joueurs
    val players = playersDF
      .select(
        col("player_id").as("id"), // ID unique du joueur
        concat_ws(" ", col("first_name"), col("last_name")).as("name"), // Combiner prénom et nom
        date_format(
          to_date(regexp_replace(col("birthdate"), "\\.", "-"), "yyyy-MM-dd"), // Remplacer '.' par '-' et convertir
          "yyyy-MM-dd"
        ).as("birth_date"), // Transformation de la date de naissance en format yyyy-MM-dd
        col("flag_code").as("country") // Code du pays
      )

    // Sélectionner et transformer les colonnes importantes pour les matches
    val matches = matchesDF
      .select(
        col("tourn_name").as("tournament"), // Nom du tournoi
        col("winner_id").as("src"), // Source : ID du vainqueur
        col("tourn_round").as("round"), // Ajout de la colonne tourn_round pour le round
        col("match_order"),
        col("match_score"),
        col("loser_id").as("dst"), // Destination : ID du perdant
        date_format(
          to_date(regexp_replace(col("tourn_start_date"), "\\.", "-"), "yyyy-MM-dd"), // Remplacer '.' par '-' et convertir
          "yyyy-MM-dd"
        ).as("start_date") // Transformation de la date du tournoi en format yyyy-MM-dd
      )

    // Retourner les DataFrames des joueurs et des matches
    (players, matches)
  }

  def createGraph(players: DataFrame, matches: DataFrame): GraphFrame = {
    // Créer le graphe avec les joueurs comme sommets et les matches comme arêtes
    val graph = GraphFrame(players, matches)
    graph
  }

  import org.apache.spark.sql.functions.{col, year}
  def getTournamentResults(graph: GraphFrame, tournamentName: String, yearTarget: Int): Unit = {
    // Filtrer les arêtes du graphe pour l'année et le tournoi donnés
    val results = graph.edges
      .filter(
        col("tournament") === tournamentName &&
          year(col("start_date")) === yearTarget // Utiliser la fonction year() correctement
      )
      .select(
        col("start_date"),
        col("round").as("round"),
        col("match_order").as("round_order"),
        col("src").as("winner_id"),
        col("dst").as("loser_id"),
        col("match_score").as("match_score")
      )

    // Afficher les résultats
    results.show(truncate = false)
  }

  def createSubGraphForTournament(graph: GraphFrame, tournamentName: String, yearTarget: Int): GraphFrame = {
    // Filtrer les arêtes pour le tournoi "nitto-atp-finals" et l'année 2020
    val filteredEdges = graph.edges
      .filter(
        col("tournament") === tournamentName &&
          year(col("start_date")) === yearTarget
      )

    // Créer un sous-graphe en gardant les sommets (players) liés aux arêtes filtrées
    val filteredVertices = graph.vertices
      .join(filteredEdges.select("src").distinct(), graph.vertices("id") === filteredEdges("src"))
      .join(filteredEdges.select("dst").distinct(), graph.vertices("id") === filteredEdges("dst"))
      .select(graph.vertices("*")).distinct() // Sélectionner les sommets uniques

    // Retourner le sous-graphe
    val subGraph = GraphFrame(filteredVertices, filteredEdges)
    subGraph
  }

  def detectGroupsForTournament(subGraph: GraphFrame, playersDF: DataFrame): Unit = {
    // Filtrer les matches pour ne conserver que ceux du tournoi "nitto-atp-finals" en 2020
    val tournamentMatches = subGraph.edges
      .filter(col("tournament") === "nitto-atp-finals" && year(col("start_date")) === 2020)

    // Récupérer les joueurs impliqués dans ces matches
    val playersInTournament = tournamentMatches
      .select("src")
      .union(tournamentMatches.select("dst"))
      .distinct()

    // Déterminer les groupes en fonction des résultats (victoires et défaites)
    val group1 = tournamentMatches
      .filter(col("src").isNotNull)
      .select("src")
      .distinct()

    val group2 = tournamentMatches
      .filter(col("dst").isNotNull)
      .select("dst")
      .distinct()

    // Jointure pour obtenir les noms des joueurs pour chaque groupe
    val group1WithNames = group1
      .join(playersDF, group1("src") === playersDF("id"))
      .select(playersDF("name").as("player_name"))

    val group2WithNames = group2
      .join(playersDF, group2("dst") === playersDF("id"))
      .select(playersDF("name").as("player_name"))

    // Afficher les joueurs du groupe 1 (par exemple, les gagnants)
    println("Groupe 1 : Joueurs gagnants")
    group1WithNames.show(truncate = false)

    // Afficher les joueurs du groupe 2 (par exemple, les perdants)
    println("Groupe 2 : Joueurs perdants")
    group2WithNames.show(truncate = false)

    // Pour une meilleure visualisation, nous pouvons aussi combiner les joueurs en 2 groupes distincts
    val group1List = group1WithNames.collect().map(r => r.getAs[String]("player_name"))
    val group2List = group2WithNames.collect().map(r => r.getAs[String]("player_name"))

    println("Liste des joueurs du Groupe 1 : " + group1List.mkString(", "))
    println("Liste des joueurs du Groupe 2 : " + group2List.mkString(", "))
  }

  def detectQualifiedPlayers(subGraph: GraphFrame, playersDF: DataFrame): Unit = {
    // Filtrer les matches pour ne conserver que ceux du tournoi "nitto-atp-finals" en 2020
    val tournamentMatches = subGraph.edges
      .filter(col("tournament") === "nitto-atp-finals" && year(col("start_date")) === 2020)

    // Compter le nombre de victoires pour chaque joueur (les victoires sont dans la colonne "src")
    val winsCount = tournamentMatches
      .groupBy("src")
      .count()
      .withColumnRenamed("count", "wins") // Renommer la colonne "count" en "wins"

    // Joindre le nombre de victoires avec le DataFrame des joueurs pour obtenir leurs noms
    val winsWithNames = winsCount
      .join(playersDF, winsCount("src") === playersDF("id"))
      .select(col("name").as("player_name"), col("wins"))

    // Trier les joueurs par le nombre de victoires (par ordre décroissant)
    val sortedWins = winsWithNames.orderBy(col("wins").desc)

    // Sélectionner les 2 meilleurs joueurs pour chaque groupe (gagnants et perdants)
    val group1Qualified = sortedWins.limit(2) // Les 2 meilleurs joueurs (gagnants)
    val group2Qualified = sortedWins.orderBy(col("wins")).limit(2) // Les 2 moins bons joueurs (perdants)

    // Afficher les joueurs qualifiés pour le Groupe 1
    println("Joueurs qualifiés pour le Groupe 1 :")
    group1Qualified.show(truncate = false)

    // Afficher les joueurs qualifiés pour le Groupe 2
    println("Joueurs qualifiés pour le Groupe 2 :")
    group2Qualified.show(truncate = false)
  }

  import org.apache.spark.sql.catalyst.encoders.RowEncoder
  import org.apache.spark.sql.Encoders

  def createImportantTournamentsGraph(spark: SparkSession, tournamentsPath: String, graph: GraphFrame): GraphFrame = {
    // Charger les données des tournois
    val tournamentsDF = spark.read.option("header", "true").option("inferSchema", "true").csv(tournamentsPath)

    // Charger un DataFrame de mapping entre les tournois et leur type
    val tournamentTypesDF = tournamentsDF
      .filter(col("tourn_type").isin("Grand Slam", "Masters 1000", "ATP 500"))
      .select("tourn_location", "tourn_type")

    // Filtrer les matches pour ne garder que ceux des tournois importants en utilisant la jointure
    val importantMatches = graph.edges
      .join(tournamentTypesDF, graph.edges("tournament") === tournamentTypesDF("tourn_location"))
      .select(graph.edges("*")) // Garder toutes les colonnes de la table edges

    // Créer le sous-graphe avec les matches filtrés
    val importantGraph = GraphFrame(graph.vertices, importantMatches)

    // Retourner le sous-graphe
    importantGraph
  }

  import org.apache.spark.sql.functions._
  import org.graphframes.GraphFrame

  def getTop10PlayersByPoints(graph: GraphFrame, tournamentsPath: String, spark: SparkSession): Unit = {
    // Charger les données des tournois
    val tournamentsDF = spark.read.option("header", "true").option("inferSchema", "true").csv(tournamentsPath)

    // Créer un DataFrame avec les points associés à chaque type de tournoi
    val tournamentPoints = Map(
      "Grand Slam" -> 2000,
      "Masters 1000" -> 1000,
      "ATP 500" -> 500
    )

    // Filtrer les tournois importants
    val importantTournaments = tournamentsDF
      .filter(col("tourn_type").isin("Grand Slam", "Masters 1000", "ATP 500"))

    // Extraire les matches de ces tournois pendant 2020
    val importantMatches = graph.edges
      .filter(col("tournament").isin(importantTournaments.select("tourn_location").as[String](Encoders.STRING).collect(): _*))
      .filter(year(col("start_date")) === 2020)

    // Ajouter une colonne de points pour chaque match en fonction du type de tournoi
    val matchesWithPoints = importantMatches
      .join(importantTournaments, importantMatches("tournament") === importantTournaments("tourn_location"))
      .withColumn("points", when(col("tourn_type") === "Grand Slam", 2000)
        .when(col("tourn_type") === "Masters 1000", 1000)
        .when(col("tourn_type") === "ATP 500", 500)
        .otherwise(0))

    // Calculer les points par joueur (src étant le gagnant)
    val playerPoints = matchesWithPoints
      .groupBy("src")
      .agg(sum("points").as("total_points"))
      .orderBy(desc("total_points"))

    // Joindre avec les informations des joueurs pour obtenir les noms
    val playerNames = graph.vertices
      .select("id", "name") // Assurez-vous que la colonne "player_name" est présente dans les vertices

    // Joindre les points avec les noms des joueurs
    val playerPointsWithNames = playerPoints
      .join(playerNames, playerPoints("src") === playerNames("id"))
      .select("name", "total_points")
      .orderBy(desc("total_points"))

    // Afficher les 10 joueurs ayant le plus de points
    playerPointsWithNames.show(10, truncate = false)
  }

  def main(args: Array[String]): Unit = {
    val (spark, sc) = getConfSpark()

    val pathMatch_scores = "C:\\Users\\nitie\\Documents\\MON BUREAU PERSONNEL\\UNIVERSITE EUROMED FES\\EIDIA-Big Data-3A\\Méga Donnée II\\Projets TP\\TP6_SparkGraphX_MD2\\Data_Sources\\match_scores_2020-2022.csv"
    val pathPlayer_overviews = "C:\\Users\\nitie\\Documents\\MON BUREAU PERSONNEL\\UNIVERSITE EUROMED FES\\EIDIA-Big Data-3A\\Méga Donnée II\\Projets TP\\TP6_SparkGraphX_MD2\\Data_Sources\\player_overviews.csv"
    val pathTournaments = "C:\\Users\\nitie\\Documents\\MON BUREAU PERSONNEL\\UNIVERSITE EUROMED FES\\EIDIA-Big Data-3A\\Méga Donnée II\\Projets TP\\TP6_SparkGraphX_MD2\\Data_Sources\\tournaments_2020-2022.csv"

    val (players, matches) = createDataFrames(spark, pathPlayer_overviews, pathMatch_scores)
    val tennisGraph = createGraph(players, matches)
    val tournamentName = "nitto-atp-finals" // Nom du tournoi  .as[String](Encoders.STRING)
    val year = 2020 // Année du tournoi
    val subGraph = createSubGraphForTournament(tennisGraph, tournamentName, year)
    val importantGraph = createImportantTournamentsGraph(spark, pathTournaments, tennisGraph)

    // 8. Afficher les 10 joueurs les mieux classés selon leur performace dans ces trois types de tournoi durant l’année 2020 en utilisant deux alternatives.
    getTop10PlayersByPoints(importantGraph, pathTournaments, spark)

    /* / 7. Créer un sous-graphe en filtrant le graphe initial par les tournois les plus importants
    val importantGraph = createImportantTournamentsGraph(spark, pathTournaments, tennisGraph)

    // Afficher les résultats ou effectuer d'autres analyses sur le sous-graphe
    println("Sous-graphe des tournois importants:")
    importantGraph.edges.show(truncate = false) // Afficher les matches du sous-graphe
    importantGraph.vertices.show(truncate = false) // Afficher les joueurs du sous-graphe */

    // 6. Lister les deux joueurs qualifiés pour chaque groupe (en se basant sur le nombre de matches gagnés).
    //detectQualifiedPlayers(subGraph, players)

    // 5. Détecter les deux groupes de ce tournoi et lister les joueurs de chaque groupe (4 joueur par groupe)
    //detectGroupsForTournament(subGraph, players)

    /* / 4. Créer un sous-graphe du graphe initial par filtrage sur le nom du tournoi (i.e. nitto-atp-finals) et l’année du tournoi (i.e. 2020).
    val tournamentName = "nitto-atp-finals" // Nom du tournoi
    val year = 2020                         // Année du tournoi
    val subGraph = createSubGraphForTournament(tennisGraph, tournamentName, year)

    // Afficher les sommets du sous-graphe
    subGraph.vertices.show(truncate = false)

    // Afficher les arêtes du sous-graphe
    subGraph.edges.show(truncate = false) */

    /* / 3. Implémenter une fonction qui affiche les résultats
    val tournamentName = "doha" // Nom du tournoi à filtrer
    val year = 2020 // Année cible
    getTournamentResults(tennisGraph, tournamentName, year) */

    /* / 2. Créer le graphe qui représentera l’ensemble des matches des tournois ATP entre 2020 et 2022.
    // Créer le graphe
    val tennisGraph = createGraph(players, matches)

    // Afficher les sommets et arêtes pour vérifier
    tennisGraph.vertices.show(5)
    tennisGraph.edges.show(5) */

    /* / 1. Créer les Dataframes des Joueurs et des Matches
    val (players, matches) = createDataFrames(spark, pathPlayer_overviews, pathMatch_scores)

    // Afficher les DataFrames pour vérifier les résultats
    players.show(5)
    matches.show(5) */







    /*/ III. Cas d’utilisation : Analyse des Vol des Compagnies Aériennes en USA
    val graph_airline = create_Airlines_Graphes(spark)
    //airlines_reference_DF(spark)
    // • Aéroports formant une communauté pour une compagnie aérienne
    val clusters = airports_community(spark, graph_airline)
    airports_clusters_detail(spark, graph_airline, clusters) */

    // • Aéroports interconnectés par compagnie aérienne
    //val airlines = interconnected_airlines(spark, graph_airline)
    //find_scc(spark, graph_airline, airlines)

    //val airL = "United Airlines"
    //val result: Row = find_large_scc_by_airline(spark, graph_airline, airL)
    //println(s"Le plus grand composant pour la compagnie aérienne $airL a une taille de : ${result.getLong(1)}")

    //interconnected_airlines(spark, graph_airline)


    // • Mauvaise journée à l'aéroport international de San Francisco (SFO)
    //bad_day_SFO(spark, graph_airline)

    // • Retards de l’aéroport ORD :
    //delay_details_ORD_CKB(spark, graph_airline)
    //delay_from_ORD(spark, graph_airline)

    // • Aéroports populaires (Départs) :
    //popular_departing_airports(spark, graph_airline)

    // • Analyse exploratoire :
    //println(graph_airline.vertices.count())
    //println(graph_airline.edges.count())


    //val graphFrame4 = create_dependence_graph(spark)
    // • Label Propagation
    //LabelPropagation(spark, graphFrame4)

    // • Strongly Connected Components
    //(spark, graphFrame4)

    // • Connected Components
    //connected_components_DG(spark, graphFrame4)

    // • Triangle Count
    //triangle_count(spark, graphFrame4)

    //val graphFrame3 = create_twitter_graph(spark)
    // • Personalized Page Rank
    //PPR_twitter(spark, graphFrame3)

    // • Page Rank
    //page_rank(spark, graphFrame3)

    // • Degree Centrality
    //degreeCentrality(graphFrame3).show(false)

    // • Shortest Path
    //val origine = "Den Haag"
    //val destination = "Ipswich"
    //Weighted_shortestPath(spark, graphFrame2, origine, destination).show(false)
    //shortest_path_graph_Transport(graphFrame2, "Amsterdam").show(false)

    // • Breadth First Search
    //BFS_Application(spark, graphFrame2)

    // II.3. Algorithmes de Graphes
    //val graphFrame2 = create_transport_graph(spark)
    //graphFrame2.vertices.show()
    //graphFrame2.edges.show()

    // II.2. Requêtes DataFrame et de Graphe
    //val graphFrame = create_grapheFrame(spark)
    //Complex_triplet_filters(graphFrame)
    //sub_graph(graphFrame)
    //stateful_queries(graphFrame)
    //find_motif(graphFrame)
    //basic_queries(graphFrame)

    // II.1. Création du Graphe
    //val graphFrame = create_grapheFrame(spark)
    //graphFrame.vertices.show()
    //graphFrame.edges.show()
    //val vertexCount = graphFrame.vertices.count()
    //println(s"Nombre de Sommets : $vertexCount")
    //val edgeCount = graphFrame.edges.count()
    //println(s"Nombre d'Arêtes : $edgeCount")

    // II.4. Algorithmes de Graphes
    //val graph = atelier_graphX_creation(sc)
    //atelier_graphX_SCC(sc, graph)
    //atelier_graphX_CC(sc, graph)
    //atelier_graphX_pageRank(sc, graph)
    //atelier_graphX_shortpath(sc, graph)

    //val graph = create_Graph(sc)
    //val graphWithRelationships = transform_Edges(graph)
    //val graphWithPersonExt = transform_Vertices(graphWithRelationships)
    //val updatedVertices = update_Graph(graphWithPersonExt)
    //val joinedGraph = join_Graph(graphWithPersonExt, updatedVertices)

    // II.3. Sélection des Sous-Graph
    //val subgraph = select_subgraph(joinedGraph)
    //println("Sous-graphe contenant uniquement les parents :")
    //subgraph.vertices.collect().foreach { case (id, personExt) =>
    //println(s"ID: $id, Name: ${personExt.name}, Age: ${personExt.age}, Children: ${personExt.children}")
    //}

    // II.2. Transformation du Graphe
    // Joindre les données mises à jour au graphe initial
    //val joinedGraph = join_Graph(graphWithPersonExt, updatedVertices)
    //println("Sommets après jointure avec les données agrégées :")
    //joinedGraph.vertices.collect().foreach { case (id, personExt) =>
      //println(s"ID: $id, Name: ${personExt.name}, Age: ${personExt.age}, Children: ${personExt.children}, " +
        //s"Friends: ${personExt.friends}, Married: ${personExt.married}")
    //}

    //val updatedVertices = update_Graph(graphWithPersonExt)
    //println("Sommets après agrégation des informations :")
    //updatedVertices.collect().foreach { case (id, (children, friends, married)) =>
      //println(s"ID: $id, Children: $children, Friends: $friends, Married: $married")
   //}

    //val graphWithRelationships = transform_Edges(graph)
    //println("Arêtes après transformation :")
    //graphWithRelationships.edges.collect().foreach { edge =>
     // println(s"${edge.srcId} -> ${edge.dstId}, relation: ${edge.attr.relation}")
    //}

    //val graphWithPersonExt = transform_Vertices(graphWithRelationships)
    //println("Sommets après transformation :")
    //graphWithPersonExt.vertices.collect().foreach { case (id, personExt) =>
    //println(s"ID: $id, Name: ${personExt.name}, Age: ${personExt.age}, Married: ${personExt.married}")
    //}

    // II.1. Création du Graphe
    //val graph = create_Graph(sc)
    //println(graph.edges.count())
    //println(graph.vertices.count())

    spark.stop()

  }
}
