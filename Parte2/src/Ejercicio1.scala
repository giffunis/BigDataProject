import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.log4j.{ Logger, Level }



object Ejercicio1 {
  def main(args: Array[String]): Unit = {
    
    // Declaración de variables
    val CSV_DELIMITER = ","
    val COL_SENSOR = 0
    val COL_DATE = COL_SENSOR + 1
    val FIRST_COL_MEASURE = COL_DATE + 1
    val sensorId = "DG1000420"
    

    disableSparkLoggin()

    // Configuración del SparkContext
    val conf = new SparkConf().setAppName("Trabajo Final- Parte 2 - Ejercicio 1").setMaster("local")
    val sc = new SparkContext(conf)
    
    // Leemos el archivo y creamos un RDD
    val data = sc.textFile("./entrada_parte2/output.csv").cache()

    /**
     * Filtramos las líneas que contengan el sensor
     * Convertimos cada línea en un array
     * Devolvemos un diccionario<fecha, medidas>
     */
    val dataFiltered = data.filter(line => line.startsWith(sensorId))  
       .map(_.split(CSV_DELIMITER))
       .map(rowFields => {
         val date = rowFields(COL_DATE)
         val measures = rowFields.slice(FIRST_COL_MEASURE, rowFields.length)
         (date, measures)
       })
      .collect().toList
      
    /**
     * Generamos la media del día siguiente cogiendo la
     * línea actual y la siguiente =S sliding(2)
     */
    val predicciones = dataFiltered.sliding(2).map {
      case List((currentDate, currentMeasures), (nextDate, nextMeasure)) =>
        nextMeasure.foreach(m => println(m))
        println(nextMeasure.length)
        println(nextMeasure.map(_.toDouble).sum)
        println(nextMeasure.map(_.toDouble).sum / nextMeasure.length)
        val average = nextMeasure.map(_.toDouble).sum / nextMeasure.length
        var fila = currentDate +: currentMeasures.toList :+ average
        fila.mkString(CSV_DELIMITER)
    }.toList
    
    // Guardar en archivo
    sc.parallelize(predicciones).saveAsTextFile("./salida_parte2/prediccion.txt")

  }
  
  
  def disableSparkLoggin() {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
  }
}