package cs6240

import org.apache.spark.SparkContext
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier,LogisticRegression, LogisticRegressionModel}

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator, BinaryClassificationEvaluator}

import org.apache.spark.SparkConf

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.mllib.tree.model.DecisionTreeModel

// Object class for prediction using RandomForestClassificationModel
object ModelFile {

  def main(args: Array[String]) {
	// Initialize spark configuration
    val conf = new SparkConf().setAppName("LoadDW")
	// Initialize spark context
    val sc = new SparkContext(conf)
	// Initialize sql context
    val sqlContext= new org.apache.spark.sql.SQLContext(sc)

	/********************** Crete Training Data *******************************/
	// read csv into rdd
    val rdd = sc.textFile(args(0)+"/*.csv")
    val data = rdd.map(_.split(",")).map(_.map(_.toDouble))

    import org.apache.spark.mllib.linalg.Vectors
    import org.apache.spark.mllib.regression.LabeledPoint

	// convert tranformed data to point <- label map for rdd1
    val labeledPoints = data.map(x => LabeledPoint(if (x.last == 1) 1 else 0,
      Vectors.dense(x.init)))
	// randomly select data for training
    val splits = labeledPoints.randomSplit(Array(1, 0), seed = 5043l)
	// read csv
    val rdd2 = sc.textFile(args(2)+"/*.csv")
	// set training data as split 0
    val trainingData = splits(0)
	
    val data2 = rdd2.map(_.split(",")).map(x => x.init).map(_.map(_.toDouble))
	// convert tranformed data to point <- label map for rdd2
    val labeledPoints2 = data2.map(x => LabeledPoint(1,Vectors.dense(x)))

	/********************** Train the model *******************************/
	
    import org.apache.spark.mllib.tree.configuration.Algo
    import org.apache.spark.mllib.tree.impurity.Gini

	// set RandomForestClassifier properties
    val algorithm = Algo.Classification
    val impurity = Gini
    val maximumDepth = 5
    val treeCount = 20
    val featureSubsetStrategy = "auto"
    val seed = 5043

    import org.apache.spark.mllib.tree.configuration.Strategy
    import org.apache.spark.mllib.tree.RandomForest

	// train the model using RandomForestClassifier
    val model = RandomForest.trainClassifier(trainingData, new Strategy(algorithm,
      impurity, maximumDepth), treeCount, featureSubsetStrategy, seed)

	/********************** Prediction *******************************/
	
	// for each labeledPoints2 predit the label
    val labeledPredictions = labeledPoints2.map { labeledPoint =>
      val predictions = model.predict(labeledPoint.features)
      (labeledPoint.label, predictions)
    }

    // save result
    labeledPredictions.map(x => x._2.toInt).saveAsTextFile(args(1))

    import org.apache.spark.mllib.evaluation.MulticlassMetrics

	// create evaluation metric based on MulticlassMetrics
    val evaluationMetrics = new MulticlassMetrics(labeledPredictions.map(x =>
      (x._1, x._2)))

	// print the accuracy of the model
    println(evaluationMetrics.precision)

  }
}
