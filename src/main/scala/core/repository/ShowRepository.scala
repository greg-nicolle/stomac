package core.repository

import com.typesafe.config.Config
import core.Repository
import org.apache.spark.sql.Dataset

case class ShowRepository[T]() extends Repository[T] {
  override def save(ds: Dataset[T], config: Config): Unit = ds.show()
}
