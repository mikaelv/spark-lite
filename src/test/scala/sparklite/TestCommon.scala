package sparklite

import org.apache.spark.sql.SparkSession

object TestCommon {
  implicit val session = SparkSession.builder().master("local").getOrCreate()

}
