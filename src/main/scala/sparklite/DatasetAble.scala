package sparklite

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count, DeclarativeAggregate}
import org.apache.spark.sql.execution.aggregate.{TypedAggregateExpression, TypedCount}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator

import scala.reflect.ClassTag
import scala.collection.breakOut

trait DatasetAble[F[_], G[_, _]] {

  def map[T, U: ClassTag : Encoder](ft: F[T])(f: T => U): F[U]

  def flatMap[T, U: ClassTag : Encoder](ft: F[T])(f: (T) => TraversableOnce[U]): F[U]

  def filter[T](ft: F[T])(f: (T) => Boolean): F[T]

  def groupByKey[T, K: Encoder : ClassTag](ft: F[T])(func: T => K): G[K, T]

  def collect[T: ClassTag](ft: F[T]): Array[T]

  def mapGroups[K, V, U: Encoder : ClassTag](kvGrouped: G[K, V])(f: (K, Iterator[V]) => U): F[U]

  def flatMapGroups[K, V, U: Encoder : ClassTag](kvGrouped: G[K, V])(f: (K, Iterator[V]) => TraversableOnce[U]): F[U]

  def agg[K, V, U1](kvGrouped: G[K, V])(col1: TypedColumn[V, U1]): F[(K, U1)]

}


object DatasetAble {
  type RDDGroup[K, V] = RDD[(K, Iterable[V])]
  type VectorGroup[K, V] = Map[K, Vector[V]]


  implicit object VectorImpl extends DatasetAble[Vector, VectorGroup] {
    def map[T, U: ClassTag : Encoder](t: Vector[T])(f: (T) => U) = t map f

    def flatMap[T, U: ClassTag : Encoder](ft: Vector[T])(f: (T) => TraversableOnce[U]): Vector[U] =
      ft flatMap f

    def filter[T](ft: Vector[T])(f: (T) => Boolean): Vector[T] =
      ft filter f

    def groupByKey[T, K: Encoder : ClassTag](ft: Vector[T])(func: (T) => K) =
      ft groupBy func

    def mapGroups[K, V, U: Encoder : ClassTag](kvGrouped: VectorGroup[K, V])(f: (K, Iterator[V]) => U) =
      kvGrouped.map { case (k, vs) => f(k, vs.iterator) }(breakOut)


    def flatMapGroups[K, V, U: Encoder : ClassTag](kvGrouped: VectorGroup[K, V])
                                                  (f: (K, Iterator[V]) => TraversableOnce[U]): Vector[U] =
      kvGrouped.flatMap { case (k, vs) => f(k, vs.iterator) }(breakOut)


    override def agg[K, V, U1](kvGrouped: VectorGroup[K, V])(col1: TypedColumn[V, U1]): Vector[(K, U1)] = {
      val aggregator = (col1.expr match {
        case AggregateExpression(typedAggExpr: TypedAggregateExpression, mode, isDistinct, resultId) =>
          typedAggExpr.aggregator

        //        case AggregateExpression(expr: DeclarativeAggregate, mode, isDistinct, resultId) =>
        //           TODO this bloody encoder is private !!
        //          val head = col1.encoder.toRow(kvGrouped.values.head)
        //          expr.initialValues // zero

        case expr => sys.error("AggregateExpression is the only type of expression supported here. col1.expr: " + expr)
      }).asInstanceOf[Aggregator[V, Any, U1]]

      kvGrouped.toVector.map { case (k, vs) =>
        val reduction = vs.foldLeft(aggregator.zero) { case (acc, v) =>
          aggregator.reduce(acc, v)
        }

        (k, aggregator.finish(reduction))
      }
    }


    def collect[T: ClassTag](ft: Vector[T]) = ft.toArray
  }

  implicit object RddImpl extends DatasetAble[RDD, RDDGroup] {
    def map[T, U: ClassTag : Encoder](t: RDD[T])(f: (T) => U): RDD[U] = t map f

    def flatMap[T, U: ClassTag : Encoder](ft: RDD[T])(f: (T) => TraversableOnce[U]): RDD[U] =
      ft flatMap f

    def filter[T](ft: RDD[T])(f: (T) => Boolean) =
      ft filter f

    def groupByKey[T, K: Encoder : ClassTag](ft: RDD[T])(func: (T) => K) =
      ft groupBy func

    override def mapGroups[K, V, U: Encoder : ClassTag](kvGrouped: RDDGroup[K, V])(f: (K, Iterator[V]) => U)
    : RDD[U] =
      kvGrouped map { case (k, it: Iterable[V]) => f(k, it.iterator) }


    def flatMapGroups[K, V, U: Encoder : ClassTag](kvGrouped: RDDGroup[K, V])(f: (K, Iterator[V]) => TraversableOnce[U])
    : RDD[U] =
      kvGrouped flatMap { case (k, it: Iterable[V]) => f(k, it.iterator) }

    def collect[T: ClassTag](ft: RDD[T]) = ft.collect()

    def agg[K, V, U1](kvGrouped: RDDGroup[K, V])(col1: TypedColumn[V, U1]): RDD[(K, U1)] = ???
  }

  implicit object DatasetImpl extends DatasetAble[Dataset, KeyValueGroupedDataset] {

    // TODO see sample usages
    //    def agg[T, U: ClassTag : Encoder](t: Dataset[T])(f: (T) => U): Dataset[U] = t.agg(Map.empty[String, String]).map.se map f

    def map[T, U: ClassTag : Encoder](t: Dataset[T])(f: (T) => U): Dataset[U] = t map f

    def flatMap[T, U: ClassTag : Encoder](t: Dataset[T])(f: (T) => TraversableOnce[U]): Dataset[U] = t flatMap f

    def filter[T](t: Dataset[T])(f: (T) => Boolean): Dataset[T] = t filter f

    def groupByKey[T, K: Encoder : ClassTag](ft: Dataset[T])(func: (T) => K): org.apache.spark.sql.KeyValueGroupedDataset[K, T] =
      ft groupByKey func

    def mapGroups[K, V, U: Encoder : ClassTag](kvGrouped: KeyValueGroupedDataset[K, V])(f: (K, Iterator[V]) => U)
    : Dataset[U] =
      kvGrouped mapGroups f

    def flatMapGroups[K, V, U: Encoder : ClassTag](kvGrouped: KeyValueGroupedDataset[K, V])(f: (K, Iterator[V]) => TraversableOnce[U])
    : Dataset[U] =
      kvGrouped flatMapGroups f


    def collect[T: ClassTag](ft: Dataset[T]): Array[T] = ft.collect()

    def agg[K, V, U1](kvGrouped: KeyValueGroupedDataset[K, V])(col1: TypedColumn[V, U1]): Dataset[(K, U1)] =
      kvGrouped.agg(col1)
  }

}


