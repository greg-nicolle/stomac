package core.utils

import job.WikiVisitsJob.session
import org.apache.spark.sql.{Dataset, Encoder}

import scala.reflect.runtime.universe._


class DatasetParser(val fieldName: String)
  extends annotation.StaticAnnotation

trait transform {

  def listProperties[T: TypeTag]: List[(String, String)] = {
    typeTag[T]
      .tpe
      .decls
      .find(_.name.toString == "<init>")
      .get
      .asMethod
      .paramLists
      .flatten
      .flatMap(x => x.annotations.flatMap(_.tree.find(_.tpe =:= typeOf[DatasetParser]).map {
        y =>
          y.children.collect({
            case Literal(Constant(fieldName: String)) =>
              fieldName -> x.name.toString
          })
      })
      ).flatten
  }

  implicit class Implicit_DataSet[T](ds: Dataset[T]) {

//    TODO Add parse dataType
    def parseTo[K: TypeTag : Encoder]: Dataset[K] = {
      import session.implicits._

      val cl = listProperties[K].map(x => $"${x._1}".as(x._2))

      ds.select(cl: _*)
        .as[K]
    }

  }

}