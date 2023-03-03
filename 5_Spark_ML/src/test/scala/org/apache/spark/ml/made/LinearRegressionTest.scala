package org.apache.spark.ml.made

import breeze.linalg.{*, DenseMatrix, DenseVector}
import com.google.common.io.Files
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec._
import org.scalatest.matchers._

class LinearRegressionTest extends AnyFlatSpec with should.Matchers with WithSpark {
  val delta: Double = 0.01
  val W: DenseVector[Double] = LinearRegressionTest._W
  val b: Double = LinearRegressionTest._b
  val y_true: DenseVector[Double] = LinearRegressionTest._y
  val df: DataFrame = LinearRegressionTest._df

  private def validateModel(model: LinearRegressionModel) = {
    model.W.size should be (W.size)
    model.W(0) should be (W(0) +- delta)
    model.W(1) should be (W(1) +- delta)
    model.W(2) should be (W(2) +- delta)
    model.b should be (b +- delta)
  }

  private def validateModelPredictions(data: DataFrame): Unit = {
    val y_pred = data.collect().map(_.getAs[Double](1))

    y_pred.length should be (100000)
    for (i <- y_pred.indices) {
      y_pred(i) should be (y_true(i) +- delta)
    }
  }

  "Estimator" should "produce functional model" in {
    val estimator = new LinearRegression()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMaxIter(100)
      .setStepSize(1.0)

    val model = estimator.fit(df)

    validateModel(model)
  }

  "Model" should "make correct predictions" in {
    val model: LinearRegressionModel = new LinearRegressionModel(
      W = Vectors.fromBreeze(W).toDense,
      b = b
    ).setFeaturesCol("features")
      .setLabelCol("label")
      .setPredictionCol("prediction")

    validateModelPredictions(model.transform(df))
  }

  "Estimator" should "work after re-read" in {
    val pipeline = new Pipeline().setStages(Array(
      new LinearRegression()
        .setFeaturesCol("features")
        .setLabelCol("label")
        .setPredictionCol("prediction")
        .setMaxIter(100)
        .setStepSize(1.0)
    ))

    val tempDir = Files.createTempDir()

    pipeline.write.overwrite().save(tempDir.getAbsolutePath)

    val reRead = Pipeline.load(tempDir.getAbsolutePath)
  
    val model = reRead.fit(df).stages(0).asInstanceOf[LinearRegressionModel]

    validateModel(model)
  }

  "Model" should "work after re-read" in {
    val pipeline = new Pipeline().setStages(Array(
      new LinearRegression()
        .setFeaturesCol("features")
        .setLabelCol("label")
        .setPredictionCol("prediction")
        .setMaxIter(100)
        .setStepSize(1.0)
    ))

    val model = pipeline.fit(df)

    val tempDir = Files.createTempDir()

    model.write.overwrite().save(tempDir.getAbsolutePath)

    val reRead: PipelineModel = PipelineModel.load(tempDir.getAbsolutePath)

    validateModelPredictions(reRead.transform(df))
  }
}

object LinearRegressionTest extends WithSpark {
  lazy val _X: DenseMatrix[Double] = DenseMatrix.rand[Double](100000, 3)
  lazy val _W: DenseVector[Double] = DenseVector(1.5, 0.3, -0.7)
  lazy val _b: Double = 1.0
  lazy val _y: DenseVector[Double] = _X * _W + _b + DenseVector.rand(100000) * 0.0001
  lazy val _df: DataFrame = createDataFrame(_X, _y)

  def createDataFrame(X: DenseMatrix[Double], y: DenseVector[Double]): DataFrame = {
    import sqlc.implicits._

    lazy val data: DenseMatrix[Double] = DenseMatrix.horzcat(X, y.asDenseMatrix.t)

    lazy val df = data(*, ::).iterator
      .map(x => (x(0), x(1), x(2), x(3)))
      .toSeq
      .toDF("feat1", "feat2", "feat3", "label")

    lazy val assembler = new VectorAssembler()
      .setInputCols(Array("feat1", "feat2", "feat3"))
      .setOutputCol("features")

    lazy val _df: DataFrame = assembler
      .transform(df)
      .select("features", "label")

    _df
  }
}
