package kapil_mtp.dceil

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import scala.Array.canBuildFrom

/**
 * Execute the ceil algorithim and save the vertices and edges in hdfs at each level.
 * Can also save locally if in local mode.
 * 
 * See CeilCoordinator for algorithm details
 */
class CeilOutput(minProgress:Int,progressCounter:Int,outputdir:String) extends CeilCoordinator(minProgress:Int,progressCounter:Int){

  var qValues = Array[(Int,Double)]()
      
  override def saveLevel(sc:SparkContext,level:Int,q:Double,graph:Graph[VertexState,Long]) = {
	  graph.vertices.saveAsTextFile(outputdir+"/level_"+level+"_vertices") 
      qValues = qValues :+ ((level,q))
      println(s"qValue: $q")
        
      // overwrite the q values at each level
      //sc.parallelize(qValues, 1).saveAsTextFile(outputdir+"/qvalues")
  }
  
}