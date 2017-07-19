package org.hdfgroup.spark

import org.apache.spark.sql.{DataFrame, DataFrameReader}

package object hdf5 {

  /*
   * Adds a method, `hdf5`, to DataFrameReader
   */
  implicit class HDF5DataFrameReader(reader: DataFrameReader) {

    def hdf5(file: String, dataset: String): DataFrame =
      reader.format("org.hdfgroup.spark.hdf5")
        .option("file path", file)
        .option("dataset", dataset)
        .load(file)

  }

}
