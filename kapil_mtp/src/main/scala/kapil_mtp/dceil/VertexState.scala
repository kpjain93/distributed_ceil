package kapil_mtp.dceil

 // VertexState contains all information needed for CEIL community detection
class VertexState extends Serializable{

  var community = -1L
  var a_c = 0L //internal edges in community c   
  var b_c = 0L //external edges associated with community c
  var n_c = 1  //number of nodes in community c
  var internal_edges = 0L //edges inside a hypernode 
  var internal_vertices = 1 //vertices inside a hypernode
  var external_edges = 0L //external edges associated with the vertex
  var changed = false  
  
  override def toString(): String = {
    community+""
  }
   
}