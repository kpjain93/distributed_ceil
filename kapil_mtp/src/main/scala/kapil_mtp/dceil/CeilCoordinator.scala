package kapil_mtp.dceil

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import scala.reflect.ClassTag
import org.apache.spark.Logging

/*
  Coordinates execution of the ceil distributed community detection process on a graph.
  
    For details see:  CEIL: A Scalable, Resolution Limit Free Approach for Detecting Communities in Large Networks  
 */
class  CeilCoordinator(minProgress:Int,progressCounter:Int) {

  
  def run[VD: ClassTag](sc:SparkContext,graph:Graph[VD,Long]) = {
    
    var ceilGraph = CeilCore.createCeilGraph(graph) 
    var N = ceilGraph.vertices.count();
    var level = -1  // number of times the graph has been compressed
    var q = 0.0
    var halt = false
    do {
	  level += 1
	  println(s"\nStarting ceil level $level")
	  
	  // label each vertex with its best community
	  val (currentQ,currentGraph,passes) = CeilCore.ceil(sc, ceilGraph,minProgress,progressCounter,N)
	  ceilGraph.unpersistVertices(blocking=false)
	  ceilGraph=currentGraph
	  
	  saveLevel(sc,level,currentQ,ceilGraph)
	  
	 // If CEIL score was increased by at least 0.001 and
	 // no.of iterations inside ceil function are greater than 2
	 // go to next phase of algorithm
	  if (passes > 2 && currentQ > q + 0.001 ){ 
	    q = currentQ
	    ceilGraph = CeilCore.compressGraph(ceilGraph)
	  }
	  else {
	    halt = true
	  }
	 
	}while ( !halt )
	finalSave(sc,level,q,ceilGraph)  
  }

  /**
   * Save the graph at the given level of compression with community labels
   * level 0 = no compression
   * 
   * override to specify save behavior
   */
  def saveLevel(sc:SparkContext,level:Int,q:Double,graph:Graph[VertexState,Long]) = {
    
  }
  
  /**
   * Complete any final save actions required
   * 
   * override to specify save behavior
   */
  def finalSave(sc:SparkContext,level:Int,q:Double,graph:Graph[VertexState,Long]) = {
    
  }
  
  
  
}