package sparklite

import DatasetAbleSpecs._
import TestCommon._
import sparklite.DatasetAble._
import sparklite._


class DatasetAbleSpecs extends org.specs2.mutable.Specification {

  val data = Vector(PersonBob, PersonRoger, PersonJohn)

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
    "flatMap" in {
      val actual: F[String] = ds.flatMap(_.children)
      actual.collect().toVector must_=== data.flatMap(_.children)
    }

    "filter" in {
      val actual: F[Person] = ds.filter(_.age >= 45)
      actual.collect().toSet must_=== Set(PersonRoger, PersonJohn)
    }

    "map" in {
      val actual: F[Int] = ds.map(_.age)
      actual.collect().toVector must_=== data.map(_.age)
    }

    "groupByKey then mapGroups" in {
      val group = ds.groupByKey(_.age)
      val actual: F[(Int, Seq[String])] =
        group.mapGroups((k, vs) => (k, vs.map(_.name).toVector))
      actual.collect().toSet must_=== Set((25, Vector("bob")), (45, Vector("roger", "john")))
    }


    "groupByKey then flatMapGroups" in {
      val group = ds.groupByKey(_.age)
      val actual: F[String] =
        group.flatMapGroups((k, vs) => vs.map(_.name).toVector)
      actual.collect().toSet must_=== Set("bob", "roger", "john")
    }
  }

}

object DatasetAbleSpecs {

  case class Person(name: String, age: Int, children: Seq[String] = Seq.empty)

  lazy val PersonBob = Person("bob", 25)
  lazy val PersonRoger = Person("roger", 45, Seq("alice"))
  lazy val PersonJohn = Person("john", 45, Seq("rob", "lucie"))
}
