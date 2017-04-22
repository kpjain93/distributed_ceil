package kapil_mtp.dceil

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import scala.reflect.ClassTag
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.Graph.graphToGraphOps
import scala.math.BigDecimal.double2bigDecimal
import org.apache.commons.lang.mutable.Mutable



/**
 * Provides low level ceil community detection algorithm functions.  Generally used by CeilCoordinator
 * to coordinate the correct execution of the algorithm though its several stages.
 * 
 * For details on the sequential algorithm see: CEIL: A Scalable, Resolution Limit Free Approach for Detecting Communities in Large Networks
 */
object CeilCore {

  
  
   /**
    * Generates a new graph of type Graph[VertexState,Long] based on an input graph of type.
    * Graph[VD,Long].  The resulting graph can be used for ceil computation.
    * 
    */
   def createCeilGraph[VD: ClassTag](graph: Graph[VD,Long]) : Graph[VertexState,Long]= {
    // Create the initial Ceil graph.  
    val nodeWeightMapFunc = (e:EdgeTriplet[VD,Long]) => Iterator((e.srcId,e.attr), (e.dstId,e.attr))
    val nodeWeightReduceFunc = (e1:Long,e2:Long) => e1+e2
    val nodeWeights = graph.mapReduceTriplets(nodeWeightMapFunc,nodeWeightReduceFunc)   
    val ceilGraph = graph.outerJoinVertices(nodeWeights)((vid,data,weightOption)=> { 
      val weight = weightOption.getOrElse(0L)
      val state = new VertexState()
      state.community = vid
      state.changed = false
      state.internal_edges = 0L
      state.external_edges = weight
      state.internal_vertices = 1
      state.n_c = 1
      state.a_c = 0L
      state.b_c = weight
      state
    }).partitionBy(PartitionStrategy.EdgePartition2D).groupEdges(_+_)
    
    return ceilGraph
   }
  
  
  
  /**
   * For a graph of type Graph[VertexState,Long] label each vertex with a community to maximize CEIL score.
   */
  def ceil(sc:SparkContext, graph:Graph[VertexState,Long], minProgress:Int=1,progressCounter:Int=1,N:Long) : (Double,Graph[VertexState,Long],Int)= {
    var ceilGraph = graph.cache()
    
    // gather community information from each vertex's local neighborhood
    var msgRDD = ceilGraph.mapReduceTriplets(sendMsg,mergeMsg)
    var activeMessages = msgRDD.count() //materializes the msgRDD and caches it in memory
     
    var updated = 0L - minProgress
    var even = false  
    var count = 0
    val maxIter = 100000 
    var stop = 0
    var updatedLastPhase = 0L
    do { 
       count += 1
	   even = ! even	   
	  
	   // label each vertex with its best community based on neighboring community information
	   val labeledVerts1 = ceilUpdate(ceilGraph,msgRDD,N,even).cache()   
     labeledVerts1.count
     
	   var labeledVerts = labeledVerts1.map( { case (vid,vdata) => (vid,vdata) })
	   // updating graph
	   ceilGraph = ceilGraph.outerJoinVertices(labeledVerts)((vid, old, newOpt) => newOpt.getOrElse(old))
	   ceilGraph.triplets.count()
     
     var hs = scala.collection.mutable.HashSet.empty[Long]
     var comm_vertices=0
     var comm_vertices1=0
     var comm_vertices2=0 
     var hyper_internal_edge=0L
     val communityUpdate = ceilGraph.triplets.flatMap(et=>{
       if (et.srcAttr.community == et.dstAttr.community){
        comm_vertices=0
        hyper_internal_edge=0L
        if(!(hs.contains(et.srcId))) {
          comm_vertices += et.srcAttr.internal_vertices;
          hyper_internal_edge+=et.srcAttr.internal_edges
          hs+=et.srcId
        }
        if(!(hs.contains(et.dstId))) {
          comm_vertices+=et.dstAttr.internal_vertices;
          hyper_internal_edge+=et.dstAttr.internal_edges
          hs+=et.dstId
        }
    	  Iterator( ( et.srcAttr.community,(et.attr+hyper_internal_edge,0L,comm_vertices)) )  // count the weight from both nodes  // count the weight from both nodes
      } 
    	else {
    	      comm_vertices1=0
    	      comm_vertices2=0
    	      if(!(hs.contains(et.srcId))) {
              comm_vertices1 = et.srcAttr.internal_vertices;
              hs+=et.srcId
            }
    	      if(!(hs.contains(et.dstId))) {
              comm_vertices2 = et.dstAttr.internal_vertices;
              hs+=et.dstId
            }
            Iterator( ( et.srcAttr.community,(0L, et.attr,comm_vertices1) ),( et.dstAttr.community,(0L, et.attr,comm_vertices2) ) )
          }   
    }).reduceByKey((a,b)=> (a._1 + b._1, a._2 + b._2, a._3 + b._3 )).cache()
    
    val updatedVerts = labeledVerts.map( {case (vid,vdata) => (vdata.community,(vid,vdata)) })
    .join(communityUpdate)
    .map( {case (community,(vertexTuple,communityTriple))=>
      if(communityTriple._1 != 0L)
        vertexTuple._2.a_c = communityTriple._1 
      else
        vertexTuple._2.a_c = vertexTuple._2.internal_edges
        vertexTuple._2.b_c = communityTriple._2
        vertexTuple._2.n_c = communityTriple._3
        ( vertexTuple._1, vertexTuple._2)
    }).cache
    updatedVerts.count()
    
	   labeledVerts.unpersist(blocking = false)
	   communityUpdate.unpersist(blocking=false)
	   
	   val prevG = ceilGraph
	   ceilGraph = ceilGraph.outerJoinVertices(updatedVerts)((vid, old, newOpt) => newOpt.getOrElse(old))
	   ceilGraph.cache()
	   
       // gather community information from each vertex's local neighborhood
	     val oldMsgs = msgRDD
       msgRDD = ceilGraph.mapReduceTriplets(sendMsg, mergeMsg).cache()
       activeMessages = msgRDD.count()  // materializes the graph by forcing computation
	 
       oldMsgs.unpersist(blocking=false)
       updatedVerts.unpersist(blocking=false)
       prevG.unpersistVertices(blocking=false)
       
       // half of the communites can switch on even cycles
       // and the other half on odd cycles (to prevent deadlocks)
       // so we only want to look for progress on odd cycles (after all vertices get a chance to move)
	   if (even) updated = 0
	   updated = updated + ceilGraph.vertices.filter(_._2.changed).count 
	   if (!even) {
	     println("  # vertices moved: "+java.text.NumberFormat.getInstance().format(updated))
	     if (updated >= updatedLastPhase - minProgress) stop += 1
	     updatedLastPhase = updated
	   }

   
    } while ( stop <= progressCounter && (even ||   (updated > 0 && count < maxIter)))
    println("\nCompleted in "+count+" cycles")
   
    var q = 0.0
    val updatedCommunity = ceilGraph.vertices
	     .map( {case (vid,vdata) => (vdata.community,vdata)}).reduceByKey((v1,v2)=>v1)
	      .map({case (cid,vdata) => 
	          var a_c = vdata.a_c
            var b_c = vdata.b_c
            var n_c = vdata.n_c
            if (n_c>1)
              q = (2 * a_c * a_c).toDouble/(N * (n_c-1)*(a_c + b_c)).toDouble
              (cid,q)
	     })      
	
	   val actualQ = updatedCommunity.values.reduce(_+_)
     
    return (actualQ,ceilGraph,count/2)
   
  }
  

  /**
   * Creates the messages passed between each vertex to convey neighborhood community data.
   */
  private def sendMsg(et:EdgeTriplet[VertexState,Long]) = {
  val m1 = (et.dstId,Map((et.srcAttr.community,et.srcAttr.a_c,et.srcAttr.b_c,et.srcAttr.n_c)->et.attr))
	val m2 = (et.srcId,Map((et.dstAttr.community,et.dstAttr.a_c,et.dstAttr.b_c,et.dstAttr.n_c)->et.attr))
	Iterator(m1, m2)    
  }
  
  
  
  /**
   *  Merge neighborhood community data into a single message for each vertex
   */
  private def mergeMsg(m1:Map[(Long,Long,Long,Int),Long],m2:Map[(Long,Long,Long,Int),Long]) ={
    val newMap = scala.collection.mutable.HashMap[(Long,Long,Long,Int),Long]()
    m1.foreach({case (k,v)=>
      if (newMap.contains(k)) newMap(k) = newMap(k) + v
      else newMap(k) = v
    })
    m2.foreach({case (k,v)=>
      if (newMap.contains(k)) newMap(k) = newMap(k) + v
      else newMap(k) = v
    })
    newMap.toMap
  }
  
  
  
   /**
   * Join vertices with community data form their neighborhood and select the best community for each vertex to maximize change in modularity.
   * Returns a new set of vertices with the updated vertex state.
   */
  private def ceilUpdate(ceilGraph:Graph[VertexState,Long], msgRDD:VertexRDD[Map[(Long,Long,Long,Int),Long]],N:Long, even:Boolean) = {
     ceilGraph.vertices.innerJoin(msgRDD)( (vid, vdata, msgs)=> {
	   var bestCommunity = vdata.community
		 var startingCommunityId = bestCommunity
		 var maxDeltaQ= deltaQ_after_removal(vdata,N,msgs)
	
	      msgs.foreach({ case( (cid,a_c,b_c,n_c), incident_edges ) => 
	      	val deltaQ = if (cid==vdata.community) 0.0 else deltaQ_after_addition(vdata,a_c,b_c,n_c,incident_edges,N) ;
	        if (deltaQ > maxDeltaQ || (deltaQ > 0 && (deltaQ == maxDeltaQ && cid > bestCommunity))){
	          maxDeltaQ = deltaQ
	          bestCommunity = cid
	        }
	      })	      
	      // only allow changes from high to low communties on even cycles and low to high on odd cycles
		  if ( vdata.community != bestCommunity && ( (even && vdata.community > bestCommunity)  || (!even && vdata.community < bestCommunity)  )  ){
		    vdata.community = bestCommunity 
		    vdata.changed = true
		  }
		  else{
		    vdata.changed = false
		  }   
	     vdata
	   })
  }
  
  private def deltaQ_after_removal(vdata:VertexState,N:Long,msgs:Map[(Long, Long, Long, Int), Long]):Double = {
    var a_c = vdata.a_c
    var b_c = vdata.b_c
    var n_c = vdata.n_c
    var curr_score= if (n_c<=1) 0.0 else (2 * a_c * a_c).toDouble/(N * (n_c-1)*(a_c + b_c)).toDouble
    var incident_edges = 0L
    msgs.foreach( { case((cid,a_c,b_c,n_c),edgeWeight) =>
      if(vdata.community==cid)
        incident_edges+=edgeWeight
    })
    n_c = n_c - vdata.internal_vertices
    a_c = a_c - vdata.internal_edges-incident_edges
    b_c = b_c - vdata.external_edges +2 * incident_edges
    var new_score=if (n_c<=1) 0.0 else (2 * a_c * a_c).toDouble/(N * (n_c-1)*(a_c + b_c)).toDouble
    return curr_score-new_score
  }
  
  private def deltaQ_after_addition(vdata:VertexState,a_c:Long,b_c:Long,n_c:Int,incident_edges:Long,N:Long): Double = {
    var curr_score= if (n_c<=1) 0.0 else (2 * a_c * a_c).toDouble/(N * (n_c-1)*(a_c + b_c)).toDouble
    var n_c1 = n_c + vdata.internal_vertices
    var a_c1 = a_c + vdata.internal_edges + incident_edges
    var b_c1 = b_c + vdata.external_edges - 2 * incident_edges
    var new_score = if (n_c1 <= 1) 0.0 else (2 * a_c1 * a_c1).toDouble/(N * (n_c1-1)*(a_c1 + b_c1)).toDouble
    
    return new_score - curr_score
  }
  
  /**
   * Returns the change in community score that would result from a vertex moving to a specified community.
   */
  
  /**
   * Compress a graph.
   */
  def compressGraph(graph:Graph[VertexState,Long],debug:Boolean=true) : Graph[VertexState,Long] = {
    println("compressed graph called");

      val newVerts = graph.vertices.values.map(vdata => (vdata.community,vdata) ).reduceByKey((v1,v2)=>v1)
      .map({case (vid,vdata)=>
      val state = new VertexState()
      state.community = vid
      state.a_c = vdata.a_c
      state.internal_edges = vdata.a_c
      state.b_c = vdata.b_c
      state.external_edges = vdata.b_c
      state.n_c = vdata.n_c
      state.internal_vertices = vdata.n_c
      state.changed = false
      (vid,state)}).cache()
    
    
    // translate each vertex edge to a community edge
    val edges = graph.triplets.flatMap(et=> {
       val src = math.min(et.srcAttr.community,et.dstAttr.community)
       val dst = math.max(et.srcAttr.community,et.dstAttr.community)
       if (src != dst) Iterator(new Edge(src, dst, et.attr))
       else Iterator.empty
    }).cache()
    
    
    // generate a new graph where each community of the previous
    // graph is now represented as a single vertex
    val compressedGraph = Graph(newVerts,edges)
      .partitionBy(PartitionStrategy.EdgePartition2D).groupEdges(_+_)
    
  
    newVerts.unpersist(blocking=false)
    edges.unpersist(blocking=false)
    return compressedGraph
   
    
  }
   
}