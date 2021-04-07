import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}

import java.io.{File, PrintWriter}
import scala.collection.mutable._

object task2 {
  def main(args: Array[String]){

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val filter_threshold = args(0).toInt//7
    val input_file_path = args(1)//"C:/Users/11921/OneDrive/FilesTransfer/DSCI 553/Assignments/Assignment4/ub_sample_data.csv"
    val betweenness_output_file_path = args(2)//"task2ScalaBetweenness.txt"
    val community_output_file_path = args(3)//"task2ScalaCommunity.txt"

    val conf = new SparkConf().setAppName("task2").setMaster("local[3]")
      .set("spark.executor.memory", "4g")
      .set("spark.driver.memory", "4g")

    val sc = new SparkContext(conf)

    val ub_raw = sc.textFile(input_file_path)
    val ub_header = ub_raw.first()
    val ub_rdd = ub_raw.filter(line=>line!=ub_header)
      .map(line=>line.split(","))
      .map(l=>(l(0), l(1)))
      .groupBy(_._1).mapValues(_.map(_._2).toList)
      .filter(pair=>pair._2.length >= filter_threshold)
    val qualified_user_list = ub_rdd.map(pair=>pair._1).distinct().collect()
    val qualified_user_bus_dict = ub_rdd.collectAsMap()

    val l = sc.parallelize(qualified_user_list)
    val user_pairs_rdd = l.cartesian(l)
      .filter(pair => pair._1 < pair._2)
      .map(pair => checkCoratedBus(pair, qualified_user_bus_dict))
      .filter(pair => pair._2 >= filter_threshold)
      .map(pair => pair._1)
      .flatMap(pair => List((pair._1, pair._2), (pair._2, pair._1)))

    var vertices_list = user_pairs_rdd.map(pair => pair._1).distinct().collect().toList
    val complete_graph = user_pairs_rdd
      .groupBy(_._1)
      .mapValues(_.map(_._2).to[ListBuffer])
      .collectAsMap()

    var betweenness_dict = betweenness(vertices_list, complete_graph)
    var sorted_dict = betweenness_dict.toList.sortBy(x => (-x._2 , x._1(0)))

    val file = new PrintWriter(new File(betweenness_output_file_path))
    for(pair<-sorted_dict){
      val line = "('" + pair._1(0) + "', '" + pair._1(1) + "'), " + pair._2.toString + "\n"
      file.write(line)
    }
    file.close()

    val partitioned_graph = user_pairs_rdd
                                                              .groupBy(_._1)
                                                              .mapValues(_.map(_._2).to[ListBuffer])
                                                              .collectAsMap()
    var two_m:Float = 0
    for((key, value)<-complete_graph){
      two_m += value.length
    }
    val A_matrix = user_pairs_rdd.collect().toSet

    val verticesList = user_pairs_rdd.map(pair => pair._1).distinct().collect().to[ListBuffer]
    var best_q = modularity(partitioned_graph, ListBuffer(verticesList), two_m, A_matrix, complete_graph)
    var best_comm = find_communities(partitioned_graph, verticesList)

    while(sorted_dict.length!=0){
      val edge_to_remove = sorted_dict(0)._1
      remove_edge(partitioned_graph, edge_to_remove)
      betweenness_dict = betweenness(vertices_list, partitioned_graph)
      sorted_dict = betweenness_dict.toList.sortBy(x => (-x._2 , x._1(0)))
      val curr_comm = find_communities(partitioned_graph, verticesList)
      val curr_q = modularity(partitioned_graph, curr_comm, two_m, A_matrix, complete_graph)
      if(curr_q > best_q){
        best_q = curr_q
        best_comm = curr_comm.clone()
      }
    }

    val comm_result = best_comm.sortBy(l => (l.length, l(0)))

    val file2 = new PrintWriter(new File(community_output_file_path))
    for(comm <- comm_result){
      var line = ""
      for(vertex<-comm){
        line += "'" + vertex + "', "
      }
      file2.write(line.slice(0,line.length-2) + "\n")
    }
    file2.close()

  }

  def modularity(partitioned_graph:scala.collection.Map[String, ListBuffer[String]], communities:ListBuffer[ListBuffer[String]], two_m:Float, A_matrix:scala.collection.Set[(String,String)], complete_graph:scala.collection.Map[String, ListBuffer[String]]):Float={
    var q:Float = 0
    for(community<-communities){
      for(i <- community){
        for(j <- community){
          val ki = complete_graph(i).length
          val kj = complete_graph(j).length
          //val edge = List(i, j).sorted
          val edge_tuple = (i, j)
          if(A_matrix.contains(edge_tuple)){
            q += 1 - (ki*kj).toFloat/two_m
          }else{
            q += 0 - (ki*kj).toFloat/two_m
          }
        }
      }
    }
    return q / two_m
  }

  def remove_edge(graph:scala.collection.Map[String, ListBuffer[String]], edge:List[String]): Unit ={
    val vertex1 = edge(0)
    val vertex2 = edge(1)
    graph(vertex1) -= vertex2
    graph(vertex2) -= vertex1
  }

  def find_communities(graph:scala.collection.Map[String, ListBuffer[String]], vertices_list:ListBuffer[String]):ListBuffer[ListBuffer[String]]={
    var found_communities = ListBuffer[ListBuffer[String]]()
    var curr_community = ListBuffer[String]()
    var found_vertices = ListBuffer[String]()
    var queue = Queue[String]()

    for(vertex<-vertices_list){
      if(!found_vertices.contains(vertex)){
        queue.enqueue(vertex)
        while(!queue.isEmpty){
          val node = queue.dequeue()
          val neighbors = graph(node)
          if(neighbors.length==0){
            found_vertices.append(node)
            curr_community.append(node)
          }else{
            for(neighbor<-neighbors){
              if(!found_vertices.contains(neighbor)){
                queue.enqueue(neighbor)
                found_vertices.append(neighbor)
                curr_community.append(neighbor)
              }
            }
          }
        }
        found_communities.append(curr_community.sorted)
        curr_community = ListBuffer[String]()
      }
    }
    return found_communities
  }


  def checkCoratedBus(pair:Tuple2[String, String], qualified_user_bus_dict:scala.collection.Map[String,List[String]]):Tuple2[Tuple2[String,String], Int]={
    val bus_list1 = qualified_user_bus_dict(pair._1).toSet
    val bus_list2 = qualified_user_bus_dict(pair._2).toSet
    val corated_num = bus_list1.intersect(bus_list2)
    return Tuple2(pair, corated_num.size)
  }

  def generatePairs(user_list:Array[String]): List[List[String]] ={
    var result = Set[List[String]]()
    for(e1 <- user_list) {
      for (e2 <- user_list) {
        if(e1 != e2){
          result += List(e1, e2).sorted
        }
      }
    }
    return result.toList
  }

  def betweenness(verticesList:List[String], completeGraph:scala.collection.Map[String,ListBuffer[String]]):Map[List[String],Float]={
    var total_edge_credit_dict = Map[List[String],Float]()
    for(root <- verticesList){
      // bfs to explore the graph and build the tree
      var visited = Set(root)
      var same_level_visited = Set[String]()
      var queue = Queue(root)
      var same_level_queue = Queue[String]()
      var bfs_result = ListBuffer[Map[String, (ListBuffer[String], ListBuffer[String])]]()
      var same_level_dict = Map[String, (ListBuffer[String], ListBuffer[String])]()
      var parents = List[String]()

      while(!queue.isEmpty){
        val node = queue.dequeue()
        val neighbors = completeGraph(node)
        for(neighbor <- neighbors){
          if(!visited.contains(neighbor)){
            if(same_level_dict.contains(node)){
              same_level_dict(node)._1.append(neighbor)
            }else{
              same_level_dict += (node -> (ListBuffer(neighbor), ListBuffer()))
            }
            same_level_visited.add(neighbor)
            if(!same_level_queue.contains(neighbor)){
              same_level_queue.enqueue(neighbor)
            }
          }else{
            if(parents.contains(neighbor)){
              if(same_level_dict.contains(node)){
                same_level_dict(node)._2.append(neighbor)
              }else{
                same_level_dict += (node -> (ListBuffer(), ListBuffer(neighbor)))
              }
            }
          }
        }
        if(queue.length==0){
          bfs_result.append(same_level_dict)
          parents = same_level_dict.keys.toList
          queue = same_level_queue.clone()
          visited = visited.union(same_level_visited)
          same_level_dict = Map[String, (ListBuffer[String], ListBuffer[String])]()
          same_level_queue = Queue[String]()
          same_level_visited = Set[String]()
        }
      }

      // bfs tree is built, proceed to GN algorithm----------------------------------------------------

      // assigning node weights to each node from top to bottom
      // initialize all node credits to one
      var node_credit_dict = Map[String, Float]()
      var node_weight_dict = Map[String, Float]()
      for(level <- bfs_result){
        for((node, adjacency) <- level){
          if(level(node)._2.length==0){
            if(node_weight_dict.contains(node)){
              node_weight_dict(node) += 1
            }else{
              node_weight_dict(node) = 1
            }
            node_credit_dict(node) = 1
          }else{
            for(parent <- adjacency._2){
              if(node_weight_dict.contains(node)){
                node_weight_dict(node) += node_weight_dict(parent)
              }else{
                node_weight_dict(node) = node_weight_dict(parent)
              }
              node_credit_dict(node) = 1
            }
          }
        }
      }
      // girvan-newman algorithm, bottom-up approach
      var edge_credit_dict = Map[List[String],Float]()
      for(level <- bfs_result.reverse){
        for((node, adjacency) <- level){
          if(adjacency._1.length !=0){
            for(child <- adjacency._1){
              val edge = List(node, child).sorted
              node_credit_dict(node) += edge_credit_dict(edge)
            }
          }
          for(parent <- adjacency._2){
            val edge = List(node, parent).sorted
            val ratio = node_weight_dict(parent) / node_weight_dict(node)
            if(edge_credit_dict.contains(edge)){
              edge_credit_dict(edge) += ratio * node_credit_dict(node)
            }else{
              edge_credit_dict(edge) = ratio * node_credit_dict(node)
            }
          }
        }
      }

      for((edge, credit) <- edge_credit_dict){
        if(total_edge_credit_dict.contains(edge)){
          total_edge_credit_dict(edge) += edge_credit_dict(edge).toFloat / 2
        }else {
          total_edge_credit_dict(edge) = edge_credit_dict(edge).toFloat / 2
        }
      }
    }

    return total_edge_credit_dict
  }
}
