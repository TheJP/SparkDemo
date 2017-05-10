// This code is an adapted version of "iX Spark 2 - MLlib Scala"
// Original Author: Jens Albrecht und Marc Fiedler
// It's pushed to github.com mainly as personal reference
import java.util

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.sql.{Row, SparkSession}

import scala.io.StdIn

object Main extends App {

  println("Starting model preparation")
  val spark = SparkSession.builder
    .appName("SparkDemo")
    .master("local[*]")
    .getOrCreate()

  // Add Progress logger
  spark.sparkContext.addSparkListener(new ExecutorLogger)
  spark.sparkContext.addSparkListener(new ProgressbarLogger)

  // Load Data
  val dataFrame = spark.read
    .option("header", true)
    .option("inferSchema", true)
    .csv("file:///D:/mushrooms_ix.csv") // https://www.dropbox.com/s/93y3f9hhjl95xdh/mushrooms_ix.csv?dl=1

  // Clean Data
  val cleanedData = dataFrame
    .na.fill(Map("bruises" -> "no"))
    .na.drop()
    .drop("veil-type")
    .cache()

  // Show Data Overview
  cleanedData.describe().show()

  // Wait with ML until key stroke
  System.in.read()

  println("Starting machine learning")
  machineLearning()

  def machineLearning(): Unit = {
    // ------------------------------ Build Pipeline (no data involved) ------------------------------
    // Prepare Features
    val label = "class"
    val features = cleanedData.columns.filter(_ != label)
    val labelIndexer = new StringIndexer()
      .setInputCol(label)
      .setOutputCol(s"i_$label")
    val featureIndexers = features
      .map(feature => new StringIndexer().setInputCol(feature).setOutputCol(s"f_$feature"))
    //      .map(_.setHandleInvalid("skip"))
    val featureColumns = featureIndexers.map(_.getOutputCol)

    // Assemble Feature Vector
    val assembler = new VectorAssembler()
      .setInputCols(featureColumns)
      .setOutputCol("features")
    val categoryIndexer = new VectorIndexer()
      .setInputCol(assembler.getOutputCol)
      .setOutputCol("categoryFeatures")
      .setMaxCategories(12)

    // Create Classifier
    val randomForestClassifier = new RandomForestClassifier()
      .setLabelCol(labelIndexer.getOutputCol)
      .setFeaturesCol(categoryIndexer.getOutputCol)
      .setPredictionCol("predictedIndex")

    // Provide means for converting predictions back to a human readable form
    val labels = labelIndexer.fit(cleanedData).labels
    val labelConverter = new IndexToString()
      .setInputCol(randomForestClassifier.getPredictionCol)
      .setOutputCol("predictedLabel")
      .setLabels(labels)

    // Setup Machine Learning Pipeline
    val pipeline = new Pipeline().setStages((
      List(labelIndexer) ++
        featureIndexers ++
        List(
          assembler,
          categoryIndexer,
          randomForestClassifier,
          labelConverter
        )).toArray)

    // ------------------------------ Do Training / Evaluation (data involved) ------------------------------
    // Separate Data for Training and Testing
    val Array(trainingData, testData) = cleanedData.randomSplit(Array(0.7, 0.3))

    // Train
    val model = pipeline.fit(trainingData)

    // Apply Trained Model on Test Data
    val predictions = model.transform(testData)

    // Evaluate Results
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol(labelIndexer.getOutputCol)
      .setPredictionCol(randomForestClassifier.getPredictionCol)
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)

    println(f"Accuracy is $accuracy%.3f")
    println()

    // Get mushroom from user
    val mushrooms = new util.ArrayList[Row]
    mushrooms.add(Row("edible" :: readMushroomFromConsole(): _*))
    val mushroomDataframe = spark.createDataFrame(mushrooms, cleanedData.schema)

    // Predict users mushroom
    val prediction = model.transform(mushroomDataframe)
    val predictedLabel = prediction.select(prediction(labelConverter.getOutputCol)).first()(0)
    println(f"Your mushroom is $predictedLabel (with a confidence of about ${accuracy * 100}%.2f%%)")
  }

  /**
    * Prompts the user to enter the features of his mushroom.
    */
  def readMushroomFromConsole(): List[_] = {
    // Map of possible options per feature
    val possibleValues = List(
      "cape-shape" -> Array("conical", "knobbed", "flat", "sunken", "bell", "convex"),
      "cape-color" -> Array("green", "yellow", "buff", "purple", "white", "gray", "pink", "red", "cinnamon", "brown"),
      "bruises" -> Array("bruises", "no"),
      "gill-color" -> Array("orange", "green", "yellow", "buff", "purple", "white", "gray", "pink", "red", "chocolate", "black", "brown"),
      "ring-number" -> Array("1", "2", "0"),
      "spore-color" -> Array("orange", "green", "yellow", "buff", "purple", "white", "chocolate", "black", "brown|")
    )
    // Prompt user input
    println("Please select the features of your mushroom")
    possibleValues.map({ case (column, possible) =>
      val prompt = s"$column (${possible.reduce(_ + ", " + _)}) [${possible.head}]: "
      val selection = Iterator.iterate(StdIn.readLine(prompt))(line => {
        println(s"Unknown option '$line' selected. Please use one of the given options")
        StdIn.readLine(prompt)
      }).collect({
        case "" => possible.head
        case option if possible.contains(option) => option
      }).next
      if (column == "ring-number") selection.toInt else selection
    })
  }

}
