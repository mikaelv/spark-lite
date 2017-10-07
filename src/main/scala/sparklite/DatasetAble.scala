package sparklite

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Encoder, KeyValueGroupedDataset}
import sparklite.KeyValueGroupedDatasetAble.RDDGroup

import scala.reflect.ClassTag

trait DatasetAble[F[_], G[_, _]] {

  def map[T, U: ClassTag : Encoder](ft: F[T])(f: T => U): F[U]

  def groupByKey[T, K: Encoder: ClassTag](ft: F[T])(func: T => K): G[K, T] = ???

}

trait KeyValueGroupedDatasetAble[F[_], G[_, _]] {
  def mapGroups[K, V, U: Encoder: ClassTag](kvGrouped: G[K, V])(f: (K, Iterator[V]) => U): F[U]

}

object KeyValueGroupedDatasetAble {
  type RDDGroup[K, V] = RDD[(K, Iterable[V])]
  implicit val datasetImpl: KeyValueGroupedDatasetAble[Dataset, KeyValueGroupedDataset] =
    new KeyValueGroupedDatasetAble[Dataset, KeyValueGroupedDataset] {
      def mapGroups[K, V, U: Encoder: ClassTag](kvGrouped: KeyValueGroupedDataset[K, V])
                                     (f: (K, Iterator[V]) => U)
      : Dataset[U] =
        kvGrouped mapGroups f
    }

  implicit val rddImpl: KeyValueGroupedDatasetAble[RDD, RDDGroup] =
    new KeyValueGroupedDatasetAble[RDD, RDDGroup] {
      override def mapGroups[K, V, U: Encoder: ClassTag](kvGrouped: RDDGroup[K, V])(f: (K, Iterator[V]) => U)
      : RDD[U] =
        kvGrouped map {case (k, it: Iterable[V]) => f(k, it.iterator)}

    }
}

object DatasetAble {
  implicit val vectorImpl: DatasetAble[Vector, Map] = new DatasetAble[Vector, Map] {
    def map[T, U: ClassTag : Encoder](t: Vector[T])(f: (T) => U) = t map f
  }

  implicit val rddImpl: DatasetAble[RDD, RDDGroup] = new DatasetAble[RDD, RDDGroup] {
    def map[T, U: ClassTag : Encoder](t: RDD[T])(f: (T) => U): RDD[U] = t map f

    override def groupByKey[T, K: Encoder: ClassTag](ft: RDD[T])(func: (T) => K) =
      ft groupBy func
  }

  implicit val datasetImpl: DatasetAble[Dataset, KeyValueGroupedDataset] = new DatasetAble[Dataset, KeyValueGroupedDataset] {

    def map[T, U: ClassTag : Encoder](t: Dataset[T])(f: (T) => U): Dataset[U] = t map f

    override def groupByKey[T, K: Encoder: ClassTag](ft: Dataset[T])(func: (T) => K): org.apache.spark.sql.KeyValueGroupedDataset[K, T] =
      ft groupByKey func
  }
}


