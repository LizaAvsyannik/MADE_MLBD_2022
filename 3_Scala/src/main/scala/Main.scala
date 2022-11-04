import breeze.linalg.{DenseMatrix, DenseVector, csvread, csvwrite}

import java.io.File

import models.LinearRegression
import model_selection.CrossValidation
object Main {
    def main(args: Array[String]): Unit = {
		if (args.length != 3) {
			println("Incorrect number of arguments")
			return
        }

		val path_to_train: File = new File(args(0))
		val path_to_test: File = new File(args(1))
		val path_to_result: File = new File(args(2))
		
		val train_data: DenseMatrix[Double] = csvread(path_to_train, skipLines = 1)
		val test_data: DenseMatrix[Double] = csvread(path_to_test, skipLines = 1)

		val X_train: DenseMatrix[Double] = train_data(::, 0 to -2).toDenseMatrix
		val y_train: DenseVector[Double] = train_data(::, -1).toDenseVector

		val model = new LinearRegression
		val cross_validation = new CrossValidation

		cross_validation.cross_validate(model, X_train, y_train)
		val predictions: DenseVector[Double] = model.predict(test_data)
		csvwrite(path_to_result, predictions.asDenseMatrix.t)
	}
}
