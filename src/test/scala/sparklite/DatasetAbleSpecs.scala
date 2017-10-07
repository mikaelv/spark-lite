package sparklite

import org.apache.spark.sql.{Dataset, KeyValueGroupedDataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import DatasetAbleSpecs._
import TestCommon._
import sparklite.DatasetAble.datasetImpl

class DatasetAbleSpecs extends org.specs2.mutable.Specification {

  // TODO factorize unit tests
  // TODO implement for Vector
  val data = Vector(Person1, Person2, Person3)
  import session.implicits._

  "the DatasetAble implementation for Spark Dataset" should {
    val ds = session.createDataset(data)
    val dsAble = DatasetAble.datasetImpl
    val groupAble = KeyValueGroupedDatasetAble.datasetImpl
    "map" in {
      val actual: Dataset[Int] = dsAble.map(ds)(_.age)
      actual.collect().toVector must_=== data.map(_.age)
    }

    "groupByKey then mapGroups" in {
      val group = dsAble.groupByKey(ds)(_.age)
      val actual: Dataset[(Int, Seq[String])] =
        groupAble.mapGroups(group)((k, vs) => (k, vs.map(_.name).toVector))
      actual.collect().toSet must_=== Set((25, Vector("bob")), (45, Vector("roger", "john")))
    }
  }

  "the DatasetAble implementation for Spark RDD" should {
    val ds = session.sparkContext.makeRDD(data)
    val dsAble = DatasetAble.rddImpl
    val groupAble = KeyValueGroupedDatasetAble.rddImpl
    "map" in {
      val actual = dsAble.map(ds)(_.age)
      actual.collect().toVector must_=== data.map(_.age)
    }

    "groupByKey then mapGroups" in {
      val group = dsAble.groupByKey(ds)(_.age)
      val actual = groupAble.mapGroups(group)((k, vs) => (k, vs.map(_.name).toVector))
      actual.collect().toSet must_=== Set((25, Vector("bob")), (45, Vector("roger", "john")))
    }
  }

}

object DatasetAbleSpecs {
  case class Person(name: String, age: Int)
  val Person1 = Person("bob", 25)
  val Person2 = Person("roger", 45)
  val Person3 = Person("john", 45)
}
