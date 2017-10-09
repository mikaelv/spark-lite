package sparklite

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Encoder, KeyValueGroupedDataset}

import scala.reflect.ClassTag
import scala.collection.breakOut

trait DatasetAble[F[_], G[_, _]] {

  def map[T, U: ClassTag : Encoder](ft: F[T])(f: T => U): F[U]

  def groupByKey[T, K: Encoder : ClassTag](ft: F[T])(func: T => K): G[K, T]

  def collect[T: ClassTag](ft: F[T]): Array[T]

  def mapGroups[K, V, U: Encoder : ClassTag](kvGrouped: G[K, V])(f: (K, Iterator[V]) => U): F[U]
}


object DatasetAble {
  type RDDGroup[K, V] = RDD[(K, Iterable[V])]
  type VectorGroup[K, V] = Map[K, Vector[V]]


  implicit val vectorImpl: DatasetAble[Vector, VectorGroup] = new DatasetAble[Vector, VectorGroup] {
    def map[T, U: ClassTag : Encoder](t: Vector[T])(f: (T) => U) = t map f

    def groupByKey[T, K: Encoder : ClassTag](ft: Vector[T])(func: (T) => K) =
      ft groupBy func

    def mapGroups[K, V, U: Encoder : ClassTag](kvGrouped: VectorGroup[K, V])(f: (K, Iterator[V]) => U) =
      kvGrouped.map { case (k, vs) => f(k, vs.iterator) }(breakOut)


    def collect[T: ClassTag](ft: Vector[T]) = ft.toArray
  }

  implicit val rddImpl: DatasetAble[RDD, RDDGroup] = new DatasetAble[RDD, RDDGroup] {
    def map[T, U: ClassTag : Encoder](t: RDD[T])(f: (T) => U): RDD[U] = t map f

    def groupByKey[T, K: Encoder : ClassTag](ft: RDD[T])(func: (T) => K) =
      ft groupBy func

    override def mapGroups[K, V, U: Encoder : ClassTag](kvGrouped: RDDGroup[K, V])(f: (K, Iterator[V]) => U)
    : RDD[U] =
      kvGrouped map { case (k, it: Iterable[V]) => f(k, it.iterator) }


    def collect[T: ClassTag](ft: RDD[T]) = ft.collect()
  }

  implicit val datasetImpl: DatasetAble[Dataset, KeyValueGroupedDataset] = new DatasetAble[Dataset, KeyValueGroupedDataset] {

    def map[T, U: ClassTag : Encoder](t: Dataset[T])(f: (T) => U): Dataset[U] = t map f

    def groupByKey[T, K: Encoder : ClassTag](ft: Dataset[T])(func: (T) => K): org.apache.spark.sql.KeyValueGroupedDataset[K, T] =
      ft groupByKey func

    def mapGroups[K, V, U: Encoder : ClassTag](kvGrouped: KeyValueGroupedDataset[K, V])
                                              (f: (K, Iterator[V]) => U)
    : Dataset[U] =
      kvGrouped mapGroups f


    def collect[T: ClassTag](ft: Dataset[T]) = ft.collect()
  }
}


