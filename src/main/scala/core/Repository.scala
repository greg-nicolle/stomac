package core

import com.typesafe.config.Config
import org.apache.spark.sql.Dataset

abstract class Repository[T]() {
  def before(config: Config): Unit = {}

  def save(ds: Dataset[T], config: Config): Unit

  def after(config: Config): Unit = {}

  def rollback(config: Config): Unit = {}
}
