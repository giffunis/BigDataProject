import org.apache.spark.{ SparkContext, SparkConf, mllib }
import org.apache.log4j.{ Logger, Level }
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors

object Ejercicio2bRandomForest {
  def main(args: Array[String]): Unit = {

    // Declaración de variables
    val CSV_DELIMITER = ","
    val COL_DATE = 0
    val COL_AVERAGE = 145
    val FIRST_COL_MEASURE = COL_DATE + 1
    val INPUT_PATH = "./entrada_parte2/ejercicio2b/predicción.txt"
    val SEED = 10 // Mismo valor que en R
    val TRAINING_PERCENTAGE = 0.7
    val TESTING_PERCENTAGE = 1 - TRAINING_PERCENTAGE
    val TREES = 20
    val MAX_DEPTH = 10
    val SPLITS = 20
    

    disableSparkLoggin()

    // Configuración del SparkContext
    val conf = new SparkConf().setAppName("Trabajo Final- Parte 2 - Ejercicio 2b").setMaster("local")
    val sc = new SparkContext(conf)

    // Leemos el archivo y creamos un RDD
    val data = sc.textFile(INPUT_PATH).cache()

    // Convertimos los datos a una estructura de datos LabeledPoint.
    // En el filtro nos aseguramos de que la última línea no la coja porque no tiene un valor para la medía del día siguiente (01/01/2014)
    val allData = data.map(_.split(CSV_DELIMITER))
      .filter(rowFields => rowFields.length > COL_AVERAGE)
      .map(rowFields => {
        val measures = rowFields.slice(FIRST_COL_MEASURE, rowFields.length - 1)
        val features = Vectors.dense(measures.map(_.toDouble))
        val label = rowFields(COL_AVERAGE).toDouble
        LabeledPoint(label, features)
      })
      .cache()

    // Dividimos los datos entre el conjunto de entrenamiento(70% y el de test (30%)
    val Array(training, test) = allData.randomSplit(Array(TRAINING_PERCENTAGE, TESTING_PERCENTAGE), seed = SEED)

    val model = RandomForest.trainRegressor(
      training,
      categoricalFeaturesInfo = Map[Int, Int](), // No se trata de una clasificación
      numTrees = TREES,
      featureSubsetStrategy = "auto",
      impurity = "variance", // para regresión
      maxDepth = MAX_DEPTH,
      maxBins = SPLITS)

    val predictionsAndLabels = test.map { point =>
      val prediction = model.predict(point.features)
      (prediction, point.label)
    }

    // Mostrar los resultados
    predictionsAndLabels.foreach {
      case (pred, label) =>
        println(f"Predicción: $pred%.4f | Valor real: $label%.4f")
    }

  }

  def disableSparkLoggin() {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
  }
}