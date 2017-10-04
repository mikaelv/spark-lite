package sparklite

import org.apache.spark.sql.{Dataset, KeyValueGroupedDataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import DatasetAbleSpecs._
import TestCommon._
import sparklite.DatasetAble.datasetImpl

class DatasetAbleSpecs extends org.specs2.mutable.Specification {
  "the DatasetAble implementation for Spark Dataset" should {
    import session.implicits._

    val data = Vector(Person1, Person2)
    val ds = session.createDataset(data)
    "map" in {
      val actual: Dataset[Int] = DatasetAble.datasetImpl.map(ds)(_.age)
      actual.collect().toVector must_=== data.map(_.age)
    }

    "groupByKey then mapGroups" in {
      val group = DatasetAble.datasetImpl.groupByKey(ds)(_.age)
      val actual: Dataset[(Int, Seq[String])] =
        KeyValueGroupedDatasetAble.datasetImpl.mapGroups(group)((k, vs) => (k, vs.map(_.name).toSeq))
      actual.collect().toVector must_=== Vector((25, Seq("bob")), (45, Seq("roger", "john")))

      // TODO does not compile with datasetImpl2. Why ??
    }
  }

}

object DatasetAbleSpecs {
  case class Person(name: String, age: Int)
  val Person1 = Person("bob", 25)
  val Person2 = Person("roger", 45)
  val Person3 = Person("john", 45)
}
