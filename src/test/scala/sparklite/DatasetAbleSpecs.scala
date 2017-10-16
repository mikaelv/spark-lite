package sparklite

import DatasetAbleSpecs._
import TestCommon._
import sparklite.DatasetAble._
import sparklite._


class DatasetAbleSpecs extends org.specs2.mutable.Specification {

  val data = Vector(Person1, Person2, Person3)

  import session.implicits._

  "the DatasetAble implementation for Spark Dataset" should {
    val ds = session.createDataset(data)
    test(ds)
  }

  "the DatasetAble implementation for Spark RDD" should {
    val ds = session.sparkContext.makeRDD(data)
    test(ds)
  }

  "the DatasetAble implementation for Vector" should {
    val ds = data
    test(ds)
  }



  def test[F[_], G[_, _]](ds: F[Person])(implicit dsAble: DatasetAble[F, G]) = {
    "map" in {
      val actual: F[Int] = ds.map(_.age)
      actual.collect().toVector must_=== data.map(_.age)
    }

    "groupByKey then mapGroups" in {
      val group = ds.groupByKey(_.age)
      val actual: F[(Int, Seq[String])] =
        group.mapGroups((k, vs) => (k, vs.map(_.name).toVector))
      dsAble.collect(actual).toSet must_=== Set((25, Vector("bob")), (45, Vector("roger", "john")))
    }
  }

}

object DatasetAbleSpecs {

  case class Person(name: String, age: Int)

  val Person1 = Person("bob", 25)
  val Person2 = Person("roger", 45)
  val Person3 = Person("john", 45)
}
