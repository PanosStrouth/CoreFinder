/**
 * Created by panos on 28/5/2015.
 */
/* CoreFinder.scala */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.io._
import java.util.Date
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext

import scala.util.control.Breaks._
import scala.math._


object CoreFinder {

  def main(args: Array[String]): Unit = {

    val logFile = "/home/panos/spark-1.2.0/README.md"
    // Create spark configuration and spark context
    val conf = new SparkConf().setAppName("CoreFinder App").setMaster("local[16]")
    val sc = new SparkContext(conf)
//    println("1")
/*
    val ssc = new StreamingContext(sc,Seconds(1))
    ssc.checkpoint("checkpoint")
*/
    val currentDir = System.getProperty("user.dir") // get the current directory
    val edgeFile = "file://" + currentDir + "/friendsterUndirected.txt"
 //     println("2") 

    // Load the edges as a graph
    val graph = GraphLoader.edgeListFile(sc, edgeFile,false,1,StorageLevel.MEMORY_AND_DISK,StorageLevel.MEMORY_AND_DISK)
 // println("3")




    /*---------------------------------------------------------------------------------------------------------*/
    /*------------------------------------HELPING FUNCTIONS----------------------------------------------------*/
    /*---------------------------------------------------------------------------------------------------------*/

    def createDegreesVertexRDD(graph: Graph[Int, Int]): VertexRDD[Int] = {
      val degrees : VertexRDD[Int] = graph.mapReduceTriplets[Int](
        // Send message to destination vertex containing src vertexId
        edge => Iterator((edge.dstId, 1)),
        // Add vertexIds
        (a, b) => ((a + b)) // Reduce Function
      )
      return degrees
    }


    def createNeighbsVertexRDD(graph: Graph[Int, Int]): VertexRDD[Array[VertexId]] = {
      val neighbs : VertexRDD[Array[VertexId]] = graph.mapReduceTriplets[Array[VertexId]](
        // Send message to destination vertex containing src vertexId
        edge => Iterator((edge.dstId,Array(edge.srcId))),
        // Add neighbors
        (a, b) =>  a++b// Reduce Function
      )
      return neighbs
    }

    //PRINT FUNCTIONS
    def printResult(vertexRDD: RDD[(Int,Int)]): Unit =
    {
      val pw = new PrintWriter(new File("results.txt"))
      vertexRDD.collect.foreach {
        case (id,int) => pw.write(id.toString() + " " + int.toString()+System.lineSeparator())//println(s"id: $id with CoreNumber: $int")
      }
      pw.close()
    }

    def printResultToFile(vertexRDD: RDD[(Int,Int)],resultName: String): Unit =
    {
      val pw = new PrintWriter(new File(resultName))
      vertexRDD.collect.foreach {
        case (id,int) => pw.write(id.toString() + " " + int.toString()+System.lineSeparator())//println(s"id: $id with CoreNumber: $int")
      }
      pw.close()
    }

    def compare(left: RDD[(Int,Int)], right: RDD[(Int,Int)]): Boolean ={
      //THIS IS AN ONE SHUFFLE SOLUTION
      val diff=left.map(_->1)
        .cogroup(right.map(_->1))
        .collect{case (a,(i1,i2)) if i1.sum != i2.sum => a -> (i1.sum-i2.sum)}
        .take(1)
        .length

      if(diff==0L) {
        println("RDDs are equal")
        return false
      }
      else{
        println("RDDs are not equal")
        return true
      }
    }

    def kbound(turn:Int,vertexId:VertexId,snvalues:Seq[Int],sn:Int):Int = {

      //val arrayofSns=snvalues.splitAt(snvalues.size-1)
      //val listOfNeighsSns = arrayofSns._1.toArray
    val listOfNeighsSns = snvalues.toArray
      //var leftPart= arrayofSns._2
      //var maximalI=leftPart(0)
      //val initialSn=leftPart(0)
      var maximalI=sn
      val initialSn=sn

      //println(s"Searching in turn:$turn for new sn value for vertexId: $vertexId with former sn value: $initialSn")
      
      if (maximalI == 0 || maximalI == 1)
      {
    //println(s"vertexId: $vertexId has new sn value : $maximalI in turn:$turn")
    //println("=========================")
        return maximalI //This means that the vertex has no neighbors
      }

    
      while(maximalI > 1)
      {
        var count = listOfNeighsSns.count(_>=maximalI)
        if (count >= maximalI)
    { 
        if(maximalI<=initialSn)
        {
            //println(s"vertexId: $vertexId has new sn value : $maximalI")
            //println("=========================")
                  return maximalI
        }
        else//this should never happen
        {
            //println(s"Unreasonable new value for sn, previous sn value: $initialSn - new value:$maximalI")
            System.exit(1)
        }
    }
        maximalI -= 1 //This makes the function quite late!! maximalI = the first smaller value than maximalI in sortedlistofSns
        // maximalI =  sortedlistofSns(count)_2 <-- This is faulty!! It accelerates but it leads to underestimation even in large deviations!
      }
      return maximalI
    }

    def SU2(input: Seq[Int]):Int = {
      val arrayofSns=input.splitAt(input.size-1)
      val listOfNeighsSns = arrayofSns._1.toArray
      var leftPart= arrayofSns._2
      val maximalI=leftPart(0)

      if (maximalI == 0 || maximalI == 1)
        return maximalI //This means that the vertex has no neighbors

      var currentThres=maximalI
      val samplesize=listOfNeighsSns.size
      var counter=0
      var lastsuccess=0    //The last successful result (the maximalI value that met the criterion)
      var lastunsuccess=0 //The last maximalI value that didn't meet the search criterion
      var lastCriterionResult= false

      var flag = true
      while(flag){
        counter=0;
        var i=0

        if(listOfNeighsSns.count(_>=currentThres)>=currentThres){
          if(lastunsuccess==0)//return only if the first currentThres is exceeded
            return currentThres;

          lastCriterionResult = true;
          flag=false
          //break
        }
        else{
          lastunsuccess = currentThres;
          currentThres=Math.floor(0.5*currentThres).toInt//decrease thres to the half to find quickly a first success solution
        }
      }

      //try to optimize the result!!
      do{
        if(lastCriterionResult){
          lastsuccess=currentThres;
          currentThres = lastsuccess+(lastunsuccess-lastsuccess)/2;
        }
        else
        {
          lastunsuccess=currentThres;
          currentThres=lastunsuccess-(lastunsuccess-lastsuccess)/2;
        }
        if(listOfNeighsSns.count(_>=currentThres)>=currentThres)
          lastCriterionResult=true
        else
          lastCriterionResult=false
      }while((lastunsuccess-lastsuccess)>1);

      return lastsuccess;
    }




    //***********************//
    //CORE FINDER DISTRIBUTED//
    //***********************//

    val EIds = graph.edges
    val degrees= createDegreesVertexRDD(graph)


    val neighbors=EIds.map{
      case(edge)=>(edge.srcId.asInstanceOf[Int],edge.dstId.asInstanceOf[Int])
    }//This is RDD2

    /*Key-Value pair RDD2
         
    VertexId neighbor
       1       4
       2       1
       1       2
       4       1  
    */

    val initialValues=degrees.map{
      case(node)=>(node._1.asInstanceOf[Int],node._2.asInstanceOf[Int])//This is RDD1
    }

    /*Key-Value pair RDD1
    
    VertexId  VertexId's SnValue
       4             1
       2             1
       1             2        
    */    

    //neighbors.persist()

    def    test(vertexId:Int,value:Int):Int={
      println(s"Searching new sn value for vertexId: $vertexId with former sn value: $value")
         
    val newvalue = value-1

      println(s"vertexId: $vertexId has new sn value : $newvalue")
      println("=========================")
    return newvalue
    }

    def Stages2(turn:Int,currentValues:RDD[(Int,Int)]): RDD[(Int,Int)]={
        println(s"TURN=$turn")
    val stage6=currentValues.map{
            case(vertexId,value)=>(vertexId,test(vertexId,value))
          }
        return stage6
    }

    def Stages(turn:Int,neighbors :RDD[(Int,Int)] ,currentValues:RDD[(Int,Int)]): RDD[(Int,Int)]={
    println(s"TURN=$turn")
      //STAGE1  RDD (join)
    /*    
        Key Value(neighbor SnValue)
         1    4,2    
         1    2,2
         4    1,1
         2    1,1        
    */    
      //println("Stage1 started")
      val stage1=neighbors.join(currentValues,16)
      //println("Stage1 finished")

      // STAGE2 RDD (exclude key value of STAGE1 RDD)
      /*
        Key(VertexId) Value(Sn)
         4            2
                 2            2
                 1            1
         1        1      
      */    
      //println("Stage2 started")
      val stage2=stage1.map{
        case(value,values)=>(values._1,values._2)
      }
      //println("Stage2 finished")
      //STAGE3 RDD
      //val stage3=stage2.groupByKey()
      //groupByKey is not so good idea due to DataBricks!

      //println("Stage31 started")
      val stage31=stage2.map{
        case(value1,value2)=>(value1,Seq(value2))
      }
      //println("Stage31 finished")
      //println("Stage32 started")
      val stage32=stage31.reduceByKey((x,y)=>(x++y))
      //println("Stage32 finished")
      // STAGE3 RDD (merge value by key)
      /*
        Key(VertexId) Sequence of Sn Values of VertexId's neighbors
         4                Seq(2)
                 2                Seq(2)
                 1                Seq(1,1)     
      */

 
      // STAGE4 RDD 
      /*
        Key(VertexId)   Sequence of Sn Values of VertexId's neighbors     SnValue
         4                    Seq(2)                   1
                 2                    Seq(2)                   1
                 1                    Seq(1,1)                2 
      */

     //STAGE4 RDD
      //val stage4=sc.union(stage3,initialValues.groupByKey())//stage3.union(initialValues.groupByKey())
      // groupByKey is not so good idea due to DataBricks..it creates a lot of shuffling

      /*
      val currentValues1=currentValues.map{
        case(value1,value2)=>(value1,Seq(value2))
      }
      */
      //println("Stage4 started")
      //val stage4=sc.union(stage32,currentValues1)
      //println("Stage4 finished")

      val stage4=stage32.join(currentValues,16)    


      //STAGE5 RDD. This RDD has the following format [VertexId, [Sn values of its neighbors] [VertexId's Sn value]]
      //val stage5=stage4.groupByKey()//groupByKey is not so good idea due to DataBricks..it creates a lot of shuffling
      //println("Stage5 started")
      //val stage5=stage4.reduceByKey((x,y)=>(x++y))
      //println("Stage5 finished")

    //TESTING current SN VALUES before kbound
       printResultToFile(currentValues,"currentSnsTurn"+turn+".txt")
    //*******END OF TESTING SN VALUES******************

      //println("Stage5 started")
      val stage6=stage4.map{
        case(vertexId,(snvalues,sn))=>(vertexId,kbound(turn,vertexId,snvalues,sn))
      }
      //println("Stage5 finished")

      return stage6


      //STAGE5 RDD. This RDD has the following format [VertexId, [Sn values of its neighbors] [VertexId's Sn value]]
      //val stage5=stage4.groupByKey()//groupByKey is not so good idea due to DataBricks..it creates a lot of shuffling
    }

    def Corefind(turn:Int): RDD[(Int,Int)]={

      val startTime = System.currentTimeMillis()
      val lastRDDfilename = "lastRDD" + (turn) + ".txt"
      val lastRDD: RDD[(Int, Int)] = sc.objectFile[(Int,Int)](lastRDDfilename)
      //printResultToFile(lastRDD,"results"+turn+".txt")
      val newRDD = Stages(turn,neighbors,lastRDD)

      if (compare(newRDD, lastRDD)) {
        val newRDDfilename = "lastRDD" + (turn+1) + ".txt"
        newRDD.saveAsObjectFile(newRDDfilename)
        val endTime = System.currentTimeMillis()
        //println("Turn elapsed:")
        //println((endTime-startTime)/1000)
        return Corefind(turn + 1)
      }
      else
        return newRDD
    //return lastRDD
    }

    val startTime = System.currentTimeMillis()

    initialValues.saveAsObjectFile("lastRDD0.txt")

    val results = Corefind(0)
    val endTime = System.currentTimeMillis()
    println("Time elapsed:")
    println((endTime-startTime)/1000)
    printResult(results) 
  }
}

