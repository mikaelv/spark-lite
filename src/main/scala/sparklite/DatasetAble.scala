package sparklite

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Encoder, KeyValueGroupedDataset}

import scala.reflect.ClassTag

trait DatasetAble[F[_]] {
  type KeyValueGroupedDataset[_, _]

  def map[T, U: ClassTag : Encoder](ft: F[T])(f: T => U): F[U]

  def groupByKey[T, K: Encoder](ft: F[T])(func: T => K): KeyValueGroupedDataset[K, T] = ???

}

trait KeyValueGroupedDatasetAble[F[_, _], DS[_]] {
  def mapGroups[K, V, U: Encoder](kvGrouped: F[K, V])(f: (K, Iterator[V]) => U): DS[U]

}

object KeyValueGroupedDatasetAble {
  implicit val datasetImpl: KeyValueGroupedDatasetAble[DatasetAble.datasetImpl.KeyValueGroupedDataset, Dataset] =
    new KeyValueGroupedDatasetAble[DatasetAble.datasetImpl.KeyValueGroupedDataset, Dataset] {
      def mapGroups[K, V, U: Encoder](kvGrouped: DatasetAble.datasetImpl.KeyValueGroupedDataset[K, V])
                                     (f: (K, Iterator[V]) => U)
      : Dataset[U] =
      // TODO how can I avoid casting? scalac should know these types are equal
        kvGrouped.asInstanceOf[KeyValueGroupedDataset[K, V]] mapGroups f
    }

  implicit val datasetImpl2: KeyValueGroupedDatasetAble[KeyValueGroupedDataset, Dataset] =
    new KeyValueGroupedDatasetAble[KeyValueGroupedDataset, Dataset] {
      def mapGroups[K, V, U: Encoder](kvGrouped: KeyValueGroupedDataset[K, V])
                                     (f: (K, Iterator[V]) => U)
      : Dataset[U] = kvGrouped mapGroups f
    }

}

object DatasetAble {
  implicit val vectorOperations: DatasetAble[Vector] = new DatasetAble[Vector] {
    def map[T, U: ClassTag : Encoder](t: Vector[T])(f: (T) => U) = t map f
  }

  implicit val rddOperations: DatasetAble[RDD] = new DatasetAble[RDD] {
    def map[T, U: ClassTag : Encoder](t: RDD[T])(f: (T) => U): RDD[U] = t map f

  }

  implicit val datasetImpl: DatasetAble[Dataset] = new DatasetAble[Dataset] {
    type KeyValueGroupedDataset[K, V] = org.apache.spark.sql.KeyValueGroupedDataset[K, V]

    def map[T, U: ClassTag : Encoder](t: Dataset[T])(f: (T) => U): Dataset[U] = t map f

    override def groupByKey[T, K: Encoder](ft: Dataset[T])(func: (T) => K): org.apache.spark.sql.KeyValueGroupedDataset[K, T] =
      ft.groupByKey(func)
  }
}


