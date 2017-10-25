import org.apache.spark.sql.Encoder

import scala.reflect.ClassTag

package object sparklite {

  // TODO extend AnyVal and repeat the implicit: more performant, better error handling
  implicit class DatasetAbleSyntax[F[_], G[_, _], T: ClassTag](val ft: F[T])(implicit dsAble: DatasetAble[F, G]) {
    def map[U: ClassTag : Encoder](f: T => U): F[U] = dsAble.map(ft)(f)

    def groupByKey[K: Encoder : ClassTag](func: T => K): G[K, T] = dsAble.groupByKey(ft)(func)

    def collect(): Array[T] = dsAble.collect(ft)
  }

  implicit class GroupAbleSyntax[F[_], G[_, _], K, V](val group: G[K, V])(implicit dsAble: DatasetAble[F, G]) {
    def mapGroups[U: Encoder : ClassTag](f: (K, Iterator[V]) => U): F[U] =
      dsAble.mapGroups(group)(f)

    def flatMapGroups[U: Encoder : ClassTag](f: (K, Iterator[V]) => TraversableOnce[U]): F[U] =
      dsAble.flatMapGroups(group)(f)
  }

}
