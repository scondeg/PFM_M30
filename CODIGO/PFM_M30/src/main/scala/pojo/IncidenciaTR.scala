package main.scala.pojo

/**
  *
  * @param id
  * @param latitudPos
  * @param longitudPos
  * @param codigo
  * @param descripcion
  * @param fechaOcurrencia
  * @param fechaCierre
  */
class IncidenciaTR(id : String,
                   latitudPos : String,
                   longitudPos : String,
                   codigo : String,
                   descripcion : String,
                   fechaOcurrencia : String,
                   fechaCierre : String
          ) extends Serializable{
  var idIncidencia = id
  var latitud = latitudPos
  var longitud = longitudPos
  var codigoIncidencia = codigo
  var desc = descripcion
  var fechaIncidencia = fechaOcurrencia
  var fechacierreIncidencia = fechaCierre

  def printIncidencia = println("id: " + idIncidencia + " - desc: " + desc)
}