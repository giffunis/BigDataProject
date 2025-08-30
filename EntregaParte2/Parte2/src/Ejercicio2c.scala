import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.log4j.{ Logger, Level }
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.KMeans

object Ejercicio2c {
  def main(args: Array[String]): Unit = {

    // Declaración de variables
    val CSV_DELIMITER = ","
    val COL_SENSOR = 0
    val COL_DATE = COL_SENSOR + 1
    val FIRST_COL_MEASURE = COL_DATE + 1
    val INPUT_PATH = "./entrada_parte2/ejercicio2c/consumo.txt"
    val OUTPUT_PATH = "./salida_parte2/ejercicio2c/clasificación.txt"
    val N_CLUSTERS = 5
    val MAX_ITERATIONS = 10

    disableSparkLoggin()

    // Configuración del SparkContext
    val conf = new SparkConf().setAppName("Trabajo Final- Parte 2 - Ejercicio 2c").setMaster("local")
    val sc = new SparkContext(conf)

    // Leemos el archivo y creamos un RDD
    val data = sc.textFile(INPUT_PATH).cache()

    // En el filtro quitamos la cabecera (empieza por Sensor)
    val dataFiltered = data.filter(line => !line.startsWith("Sensor"))
      .map(_.split(CSV_DELIMITER))
      .map(rowFields => {
        val sensor = rowFields(COL_SENSOR)
        val date = rowFields(COL_DATE)
        val measures = rowFields.slice(FIRST_COL_MEASURE, rowFields.length)
        val features = Vectors.dense(measures.map(_.toDouble))
        (sensor, date, features)
      })
      .cache()

    // Extraer solo las medidas para entrenar el modelo
    val measuresRDD = dataFiltered.map(_._3)
    val clusters = KMeans.train(measuresRDD, N_CLUSTERS, MAX_ITERATIONS)

    //Obtenemos el error
    //val WSSSE = clusters.computeCost(measuresRDD)

    // Generamos el conjunto de datos
    val labeledData = dataFiltered.map {
      case (sensor, date, measures) =>
        val label = clusters.predict(measures)
        (sensor, date, label)
    }

    // Guardamos en archivo
    val output = labeledData.map {
      case (sensor, date, label) => s"$sensor$CSV_DELIMITER$date$CSV_DELIMITER$label"
    }

    output.saveAsTextFile(OUTPUT_PATH)

  }

  def disableSparkLoggin() {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
  }

}