package model_selection

import breeze.linalg.{DenseMatrix, DenseVector, inv}
import breeze.stats.mean
import breeze.numerics.pow

import java.io.{PrintWriter, File}

import models.LinearRegression

class CrossValidation {
    def mse(y_true: DenseVector[Double], y_pred: DenseVector[Double]): Double = {
        mean(pow((y_pred - y_true), 2))
    }

    def cross_validate(model: LinearRegression, 
                     X: DenseMatrix[Double], y: DenseVector[Double], 
                     n_folds: Int = 5, refit: Boolean = true):Unit = {
        val cv_results_file = new PrintWriter(new File("results/cross_val.txt" ))
        val fold_size: Int = X.rows / n_folds

        for ( i <- 0 to n_folds - 1 ) {
            val X_out: DenseMatrix[Double] = X(i to i + fold_size - 1, ::).toDenseMatrix
            val y_out: DenseVector[Double] = y(i to i + fold_size - 1)
            
            val fold_indices: IndexedSeq[Int] = IndexedSeq.range(0, i * fold_size) ++ IndexedSeq.range((i + 1) * fold_size, X.rows)
            val X_fold: DenseMatrix[Double] = X(fold_indices, ::).toDenseMatrix
            val y_fold: DenseVector[Double] = y(fold_indices).toDenseVector
            
            model.fit(X_fold, y_fold)
            val mse_score: Double = mse(y_out, model.predict(X_out))
            cv_results_file.write(s"Fold $i, MSE $mse_score\n")
        }
        cv_results_file.close

        if (refit) {
            model.predict(X)
        }
    }
}
