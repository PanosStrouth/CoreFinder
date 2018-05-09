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

    val currentDir = System.getProperty("user.dir") // get the current directory
    val edgeFile = "file://" + currentDir + "/friendsterUndirected.txt"

    // Load the edges as a graph
    val graph = GraphLoader.edgeListFile(sc, edgeFile,false,1,StorageLevel.MEMORY_AND_DISK,StorageLevel.MEMORY_AND_DISK)

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

    def kbound(turn:Int,vertexId:VertexId,snvalues:Seq[Int],sn:Int):Int = 
    {
      val listOfNeighsSns = snvalues.toArray
      var maximalI=sn
      val initialSn=sn

      if (maximalI == 0 || maximalI == 1)
      {
        return maximalI //This means that the vertex has no neighbors
      }

    
      while(maximalI > 1)
      {
        var count = listOfNeighsSns.count(_>=maximalI)
        if (count >= maximalI)
        { 
           if(maximalI<=initialSn)
           {
                  return maximalI
           }
           else//this should never happen
           {
            System.exit(1)
           }
        }
        maximalI -= 1 //This makes the function quite late!! maximalI = the first smaller value than maximalI in sortedlistofSns
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
      while(flag)
      {
        counter=0;
        var i=0

        if(listOfNeighsSns.count(_>=currentThres)>=currentThres)
        {
          if(lastunsuccess==0)//return only if the first currentThres is exceeded
            return currentThres;

          lastCriterionResult = true;
          flag=false
        }
        else{
          lastunsuccess = currentThres;
          currentThres=Math.floor(0.5*currentThres).toInt//decrease thres to the half to find quickly a first success solution
        }
      }

      //try to optimize the result!!
      do{
        if(lastCriterionResult)
        {
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

    def Stages2(turn:Int,currentValues:RDD[(Int,Int)]): RDD[(Int,Int)]=
    {
        println(s"TURN=$turn")
        val stage6=currentValues.map
        {
            case(vertexId,value)=>(vertexId,test(vertexId,value))
        }
        return stage6
    }

    def Stages(turn:Int,neighbors :RDD[(Int,Int)] ,currentValues:RDD[(Int,Int)]): RDD[(Int,Int)]=
    {
      //STAGE1  RDD (join)
      /*    
        Key Value(neighbor SnValue)
         1    4,2    
         1    2,2
         4    1,1
         2    1,1        
      */    
      val stage1=neighbors.join(currentValues,16)

      // STAGE2 RDD (exclude key value of STAGE1 RDD)
      /*
        Key(VertexId) Value(Sn)
         4            2
         2            2
         1            1
         1            1      
      */    
      val stage2=stage1.map{
        case(value,values)=>(values._1,values._2)
      }
      //STAGE3 RDD
      val stage31=stage2.map{
        case(value1,value2)=>(value1,Seq(value2))
      }
      val stage32=stage31.reduceByKey((x,y)=>(x++y))
      val stage4=stage32.join(currentValues,16)    
      printResultToFile(currentValues,"currentSnsTurn"+turn+".txt")
    //*******END OF TESTING SN VALUES******************

      val stage6=stage4.map{
        case(vertexId,(snvalues,sn))=>(vertexId,kbound(turn,vertexId,snvalues,sn))
      }
      return stage6
    }

    def Corefind(turn:Int): RDD[(Int,Int)]=
    {
      val startTime = System.currentTimeMillis()
      val lastRDDfilename = "lastRDD" + (turn) + ".txt"
      val lastRDD: RDD[(Int, Int)] = sc.objectFile[(Int,Int)](lastRDDfilename)
      val newRDD = Stages(turn,neighbors,lastRDD)

      if (compare(newRDD, lastRDD)) {
        val newRDDfilename = "lastRDD" + (turn+1) + ".txt"
        newRDD.saveAsObjectFile(newRDDfilename)
        val endTime = System.currentTimeMillis()
        return Corefind(turn + 1)
      }
      else
        return newRDD
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

