package dataingest.datasink

import dataingest.datasink.phoenix.MultiTableProductRDDFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow

/**
  * Created by dingbingbing on 4/24/18.
  */
package object datasink {

  implicit def toProductRDDFunctions[A <: InternalRow](rdd: RDD[(String, A)]): MultiTableProductRDDFunctions[A] = {
    new MultiTableProductRDDFunctions[A](rdd)
  }
}
