import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession}


object Projet_Tennis {

  // Fonction pour configurer Spark et obtenir le SparkContext
  def getConfSpark(): (SparkSession, SparkContext) = {
    val spark = SparkSession.builder()
      .appName("Projet_Tennis")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    // Récupération du SparkContext depuis SparkSession
    val sc = spark.sparkContext
    (spark, sc)
  }

  // Définition des case classes
  case class Player(name: String, country: String)
  case class Match(matchType: String, points: Int, head2HeadCount: Int)

  // Fonction pour créer les sommets et les arêtes
  def createGraph(spark: SparkSession): Graph[Player, Match] = {
    // Créer les sommets des joueurs
    val players = spark.sparkContext.parallelize(Array(
      (1L, Player("Novak Djokovic", "SRB")),
      (3L, Player("Roger Federer", "SUI")),
      (5L, Player("Tomas Berdych", "CZE")),
      (7L, Player("Kei Nishikori", "JPN")),
      (11L, Player("Andy Murray", "GBR")),
      (15L, Player("Stan Wawrinka", "SUI")),
      (17L, Player("Rafael Nadal", "ESP")),
      (19L, Player("David Ferrer", "ESP"))
    ): Array[(Long, Player)])

    // Création des arêtes des matchs
    val matches = spark.sparkContext.parallelize(Array(
      Edge(1L, 5L, Match("G1", 1, 1)),
      Edge(1L, 7L, Match("G1", 1, 1)),
      Edge(3L, 1L, Match("G1", 1, 1)),
      Edge(3L, 5L, Match("G1", 1, 1)),
      Edge(3L, 7L, Match("G1", 1, 1)),
      Edge(7L, 5L, Match("G1", 1, 1)),
      Edge(11L, 19L, Match("G2", 1, 1)),
      Edge(15L, 11L, Match("G2", 1, 1)),
      Edge(15L, 19L, Match("G2", 1, 1)),
      Edge(17L, 11L, Match("G2", 1, 1)),
      Edge(17L, 15L, Match("G2", 1, 1)),
      Edge(17L, 19L, Match("G2", 1, 1)),
      Edge(3L, 15L, Match("S", 5, 1)),
      Edge(1L, 17L, Match("S", 5, 1)),
      Edge(1L, 3L, Match("F", 11, 1))
    ): Array[Edge[Match]])

    // Création du graph avec les sommets et les arêtes
    val graph = Graph(players, matches)
    graph
  }

  def findTournamentWinner(graph: Graph[Player, Match]): String = {
    // Créer un dictionnaire pour stocker le total des points de chaque joueur
    val playerPoints = scala.collection.mutable.Map[Long, Int]()

    // Parcours des arêtes pour accumuler les points des joueurs
    graph.edges.collect().foreach { edge =>
      val matchDetails = edge.attr
      val player1Id = edge.srcId
      val player2Id = edge.dstId

      // Accumuler les points pour le joueur 1
      playerPoints(player1Id) = playerPoints.getOrElse(player1Id, 0) + matchDetails.points

      // Accumuler les points pour le joueur 2
      playerPoints(player2Id) = playerPoints.getOrElse(player2Id, 0) + matchDetails.points
    }

    // Trouver le joueur avec le plus grand nombre de points
    val winnerId = playerPoints.maxBy(_._2)._1

    // Récupérer le nom du joueur vainqueur
    val winnerName = graph.vertices.lookup(winnerId).head.name
    winnerName
  }

  def findRepeatedMatches(graph: Graph[Player, Match]): Unit = {
    // Créer un dictionnaire pour suivre les rencontres entre les joueurs
    val matchCount = scala.collection.mutable.Map[(Long, Long), Int]()

    // Parcours des arêtes pour compter les rencontres entre joueurs
    graph.edges.collect().foreach { edge =>
      val matchDetails = edge.attr
      val player1Id = edge.srcId
      val player2Id = edge.dstId

      // Filtrer uniquement les matchs de groupe (matchType == "G1")
      if (matchDetails.matchType == "G1") {
        // On s'assure que les paires de joueurs sont toujours dans un ordre particulier pour éviter les doublons
        val pair = if (player1Id < player2Id) (player1Id, player2Id) else (player2Id, player1Id)

        // Incrémenter le compteur pour cette rencontre
        matchCount(pair) = matchCount.getOrElse(pair, 0) + 1
      }
    }

    // Afficher les joueurs qui se sont affrontés plus d'une fois
    matchCount.foreach { case ((player1Id, player2Id), count) =>
      if (count > 1) {
        val player1Name = graph.vertices.lookup(player1Id).head.name
        val player2Name = graph.vertices.lookup(player2Id).head.name
        println(s"${player1Name} et ${player2Name} ont joué $count fois.")
      }
    }
  }

  def listPlayersByMatchResults(graph: Graph[Player, Match]): Unit = {
    // Créer des ensembles pour suivre les joueurs gagnants et perdants
    val winners = scala.collection.mutable.Set[Long]()
    val losers = scala.collection.mutable.Set[Long]()
    val both = scala.collection.mutable.Set[Long]()

    // Parcours des arêtes pour déterminer les gagnants et les perdants
    graph.edges.collect().foreach { edge =>
      val matchDetails = edge.attr
      val player1Id = edge.srcId
      val player2Id = edge.dstId

      // Vérifier qui a gagné et qui a perdu
      if (matchDetails.points > 0) {
        winners.add(player1Id)
        losers.add(player2Id)
      } else {
        winners.add(player2Id)
        losers.add(player1Id)
      }
    }

    // Trouver les joueurs qui ont gagné et perdu au moins un match
    winners.foreach { winnerId =>
      if (losers.contains(winnerId)) {
        both.add(winnerId)
      }
    }

    // Afficher les résultats
    println("Joueurs qui ont gagné au moins un match :")
    winners.foreach { winnerId =>
      val playerName = graph.vertices.lookup(winnerId).head.name
      println(playerName)
    }

    println("\nJoueurs qui ont perdu au moins un match :")
    losers.foreach { loserId =>
      val playerName = graph.vertices.lookup(loserId).head.name
      println(playerName)
    }

    println("\nJoueurs qui ont gagné et perdu au moins un match :")
    both.foreach { playerId =>
      val playerName = graph.vertices.lookup(playerId).head.name
      println(playerName)
    }
  }

  def listPlayersWithoutVictory(graph: Graph[Player, Match]): Unit = {
    // Créer un ensemble pour suivre les joueurs qui ont gagné au moins un match
    val winners = scala.collection.mutable.Set[Long]()

    // Parcours des arêtes pour déterminer les gagnants
    graph.edges.collect().foreach { edge =>
      val matchDetails = edge.attr
      val player1Id = edge.srcId
      val player2Id = edge.dstId
      // Vérifier qui a gagné
      if (matchDetails.points > 0) {
        winners.add(player1Id)
      } else {
        winners.add(player2Id)
      }
    }
    // Parcours des joueurs et afficher ceux qui n'ont pas gagné de match
    graph.vertices.collect().foreach { vertex =>
      if (!winners.contains(vertex._1)) { // vertex._1 est l'ID du joueur
        val playerName = vertex._2.name // vertex._2 est l'objet Player
        println(s"$playerName n'a aucune victoire.")
      }
    }
  }

  def listPlayersWithoutLoss(graph: Graph[Player, Match]): Unit = {
    // Créer un ensemble pour suivre les joueurs qui ont perdu au moins un match
    val losers = scala.collection.mutable.Set[Long]()

    // Parcours des arêtes pour déterminer les perdants
    graph.edges.collect().foreach { edge =>
      val matchDetails = edge.attr
      val player1Id = edge.srcId
      val player2Id = edge.dstId

      // Vérifier qui a perdu
      if (matchDetails.points <= 0) {
        losers.add(player1Id) // Si points <= 0, player1 a perdu
      } else {
        losers.add(player2Id) // Sinon, player2 a perdu
      }
    }

    // Parcours des joueurs et afficher ceux qui n'ont pas perdu de match
    graph.vertices.collect().foreach { vertex =>
      // Si l'ID du joueur n'est pas dans l'ensemble 'losers', cela signifie qu'il n'a jamais perdu
      if (!losers.contains(vertex._1)) { // vertex._1 est l'ID du joueur
        val playerName = vertex._2.name // vertex._2 est l'objet Player
        println(s"${playerName} n'a subi aucune perte.")
      }
    }
  }

  def findTopThreeImportantPlayers(graph: Graph[Player, Match]): Unit = {
    // Inverser le graphe pour que le gagnant devienne le sommet de destination
    val reversedGraph = graph.reverse

    // Récupérer les joueurs triés par le degré entrant (in-degree)
    val inDegrees: VertexRDD[Int] = reversedGraph.inDegrees
    val importantPlayers: RDD[(VertexId, (Int, Player))] = inDegrees.join(graph.vertices).sortBy(_._2._1, ascending = false)

    // Afficher les trois joueurs les plus importants (avec le plus grand in-degree)
    importantPlayers.take(3).foreach { case (playerId, (inDegree, player)) =>
      println(s"Joueur: ${player.name}, Pays: ${player.country}, Classement (importance): $inDegree")
    }
  }

  def main(args: Array[String]): Unit = {
    val (spark, sc) = getConfSpark()

    // Appel de la fonction pour créer le graph
    val graph = createGraph(spark)

    // I.13. Trouver et afficher les informations des trois joueurs les plus importants en se basant sur leur classement
    findTopThreeImportantPlayers(graph)

    /* / I.12. Afficher les joueurs qui n’ont subi aucune perte.
    listPlayersWithoutLoss(graph) */

    /* / I.11. Afficher les joueurs qui n’ont aucune victoire.
    listPlayersWithoutVictory(graph) */

    /* / 1.10. Lister les joueurs qui ont gagné au moins un match, les joueurs qui ont perdu au moins un match et les joueurs qui ont gagné au moins un match et perdu au moins un match
    listPlayersByMatchResults(graph) */

    /* / 1.9. joueurs qui se sont affrontés plus d'une fois dans ce tournoi
    findRepeatedMatches(graph) */

    /* / 1.8. Trouver le vainqueur du tournoi en se basant sur le plus grand nombre de points gagnés par le joueur
    val winner = findTournamentWinner(graph)
    println(s"Le vainqueur du tournoi est : $winner") */

    /* / 1.7. Créer un dictionnaire pour stocker le total des points de chaque joueur
    val playerPoints = scala.collection.mutable.Map[Long, Int]()

    // Parcours des arêtes pour accumuler les points des joueurs
    graph.edges.collect().foreach { edge =>
      val matchDetails = edge.attr
      val player1Id = edge.srcId
      val player2Id = edge.dstId

      // Accumuler les points pour le joueur 1
      playerPoints(player1Id) = playerPoints.getOrElse(player1Id, 0) + matchDetails.points

      // Accumuler les points pour le joueur 2
      playerPoints(player2Id) = playerPoints.getOrElse(player2Id, 0) + matchDetails.points
    }
    // Parcours des joueurs pour afficher leur total de points
    playerPoints.foreach { case (playerId, totalPoints) =>
      val playerName = graph.vertices.lookup(playerId).head.name
      println(s"$playerName a gagné un total de $totalPoints points")
    } */

    /* / 1.6. Afficher le vainqueur final avec les points du match
    graph.edges.collect().filter(edge => edge.attr.matchType == "F").foreach { edge =>
      val matchDetails = edge.attr
      val player1Id = edge.srcId
      val player2Id = edge.dstId

      // Trouver les noms des joueurs
      val player1Name = graph.vertices.lookup(player1Id).head.name
      val player2Name = graph.vertices.lookup(player2Id).head.name

      // Déterminer le vainqueur du match
      val winner = if (matchDetails.points > 0) player1Name else player2Name

      // Affichage du vainqueur et des points du match
      println(s"Vainqueur final : $winner, Points : ${matchDetails.points}")
    } */

    /* / 1.5. Lister tous les vainqueurs des demi-finales avec les points du match
    graph.edges.collect().filter(edge => edge.attr.matchType == "S").foreach { edge =>
      val matchDetails = edge.attr
      val player1Id = edge.srcId
      val player2Id = edge.dstId

      // Trouver les noms des joueurs
      val player1Name = graph.vertices.lookup(player1Id).head.name
      val player2Name = graph.vertices.lookup(player2Id).head.name

      // Déterminer le vainqueur du match
      val winner = if (matchDetails.points > 0) player1Name else player2Name

      // Affichage du vainqueur et des points du match
      println(s"Vainqueur : $winner, Points : ${matchDetails.points}")
    } */

    /* / 1.4. Lister tous les vainqueurs du Groupe 2 avec le total des points des matches
    graph.edges.collect().filter(edge => edge.attr.matchType == "G2").foreach { edge =>
      val matchDetails = edge.attr
      val player1Id = edge.srcId
      val player2Id = edge.dstId

      // Trouver les noms des joueurs
      val player1Name = graph.vertices.lookup(player1Id).head.name
      val player2Name = graph.vertices.lookup(player2Id).head.name

      // Déterminer le vainqueur du match
      val winner = if (matchDetails.points > 0) player1Name else player2Name

      // Affichage du vainqueur et des points du match
      println(s"Vainqueur : $winner, Points : ${matchDetails.points}")
    } */

    /* / I.3. Lister tous les vainqueurs du Groupe 1 avec le total des points des matches
    graph.edges.collect().filter(edge => edge.attr.matchType == "G1").foreach { edge =>
      val matchDetails = edge.attr
      val player1Id = edge.srcId
      val player2Id = edge.dstId

      // Trouver les noms des joueurs
      val player1Name = graph.vertices.lookup(player1Id).head.name
      val player2Name = graph.vertices.lookup(player2Id).head.name

      // Déterminer le vainqueur du match
      val winner = if (matchDetails.points > 0) player1Name else player2Name

      // Affichage du vainqueur et des points du match
      println(s"Vainqueur : $winner, Points : ${matchDetails.points}")
    } */

    /* / I.2. Lister tous les matchs sous la forme (noms des joueurs, type de match et le résultat)
    graph.edges.collect().foreach { edge =>
      val matchDetails = edge.attr
      val player1Id = edge.srcId
      val player2Id = edge.dstId

      // Trouver les noms des joueurs
      val player1Name = graph.vertices.lookup(player1Id).head.name
      val player2Name = graph.vertices.lookup(player2Id).head.name

      // Déterminer le vainqueur et le perdant
      val (winner, loser) = if (matchDetails.points > 0) (player1Name, player2Name) else (player2Name, player1Name)

      // Affichage du résultat
      println(s"$winner a battu $loser lors du match ${matchDetails.matchType}")
    } */

    /* / I.1. Lister tous les détails des matchs
    println("Détails des matchs :")
    graph.edges.collect().foreach { edge =>
      val matchDetails = edge.attr
      val player1 = graph.vertices.collect().find(v => v._1 == edge.srcId).map(_._2).getOrElse(Player("Unknown", "Unknown"))
      val player2 = graph.vertices.collect().find(v => v._1 == edge.dstId).map(_._2).getOrElse(Player("Unknown", "Unknown"))
      println(s"Match entre ${player1.name} (${player1.country}) et ${player2.name} (${player2.country}) :")
      println(s"  Type du match : ${matchDetails.matchType}")
      println(s"  Points : ${matchDetails.points}")
      println(s"  Confrontations directes : ${matchDetails.head2HeadCount}")
      println("--------")
    } */

    spark.stop()
  }
}
