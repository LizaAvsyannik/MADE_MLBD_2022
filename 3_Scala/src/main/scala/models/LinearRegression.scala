package models

import breeze.linalg.{DenseMatrix, DenseVector, inv}

class LinearRegression {
    var weights: DenseVector[Double] = DenseVector.zeros[Double](size = 0) 

    def fit(X: DenseMatrix[Double], y: DenseVector[Double]): Unit = {
        weights = inv(X.t * X) * X.t * y
    }

    def predict(X: DenseMatrix[Double]): DenseVector[Double] = {
        return X * weights
    }

    def fit_predict(X: DenseMatrix[Double], y: DenseVector[Double]): DenseVector[Double] = {
        fit(X, y)
        return predict(X)
    }
}
