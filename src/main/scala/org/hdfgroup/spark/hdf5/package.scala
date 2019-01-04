package org.hdfgroup.spark

import org.apache.spark.sql.{DataFrame, DataFrameReader}

package object hdf5 {

  // Adds a method, `hdf5`, to DataFrameReader
  implicit class HDF5DataFrameReader(reader: DataFrameReader) {

    def hdf5(flist: String, path: String, dataset: String): DataFrame =
      reader.format("org.hdfgroup.spark.hdf5")
        .option("files", flist)
        .option("path", path)
        .option("dataset", dataset)
        .load()
  }
}
