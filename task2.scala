import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}

import java.io.{File, PrintWriter}
import scala.collection.mutable._

object task2 {
  def main(args: Array[String]){

    //Logger.getLogger("org").setLevel(Level.ERROR)
    //Logger.getLogger("akka").setLevel(Level.ERROR)

    val filter_threshold = args(0).toInt //7
    val input_file_path = args(1)//"C:/Users/11921/OneDrive/FilesTransfer/DSCI 553/Assignments/Assignment4/ub_sample_data.csv"
    val betweenness_output_file_path = args(2)//"task2ScalaBetweenness.txt"
    val community_output_file_path = args(3)//"task2ScalaCommunity.txt"

    val conf = new SparkConf().setAppName("task2").setMaster("local[*]")
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

    //val qualified_user_pairs = generatePairs(qualified_user_list)
    val qualified_user_pairs:List[(String, String)] = qualified_user_list.combinations(2).toList
    val user_pairs_rdd = sc.parallelize(qualified_user_pairs)
      .map(pair => checkCoratedBus(pair, qualified_user_bus_dict))
      .filter(pair => pair._2 >= filter_threshold)
      .map(pair => pair._1)
      .flatMap(pair => List((pair._1, pair._2), (pair._2, pair._1)))

    val vertices_list = user_pairs_rdd.map(pair => pair._1).distinct().collect().toList
    val complete_graph = user_pairs_rdd
      .groupBy(_._1)
      .mapValues(_.map(_._2).to[ListBuffer])
      .collectAsMap()

    val betweenness_dict = betweenness(vertices_list, complete_graph)
    val sorted_dict = betweenness_dict.toList.sortBy(x => (-x._2 , x._1(0)))

    val file = new PrintWriter(new File(betweenness_output_file_path))
    for(pair<-sorted_dict){
      val line = "('" + pair._1(0) + "', '" + pair._1(1) + "'), " + pair._2.toString + "\n"
      file.write(line)
    }
    file.close()

    val file2 = new PrintWriter(new File(community_output_file_path))
    file2.write("Not implemented")
    file2.close()

  }

  def checkCoratedBus(pair:Tuple2[String, String], qualified_user_bus_dict:scala.collection.Map[String,List[String]]):Tuple2[Tuple2[String,String], Int]={
    val bus_list1 = qualified_user_bus_dict(pair._1).toSet
    val bus_list2 = qualified_user_bus_dict(pair._2).toSet
    val corated_num = bus_list1.intersect(bus_list2)
    return Tuple2(pair, corated_num.size)
  }

  def generatePairs(user_list:Array[String]): ListBuffer[(String, String)] ={
    val result = ListBuffer[(String, String)]()
    for(e1 <- user_list) {
      for (e2 <- user_list) {
        result.append(Tuple2(e1, e2))
      }
    }
    return result
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
