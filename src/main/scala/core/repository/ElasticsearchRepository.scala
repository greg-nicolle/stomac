package core.repository

import com.typesafe.config.Config
import core.Repository
import org.apache.spark.sql.{Dataset, SaveMode}

case class ElasticsearchRepository[T](index: String, indexType: String) extends Repository[T] {

  override def save(ds: Dataset[T], config: Config): Unit = {

    val esHost: String = config.getString("elasticsearch.host")
    val esPort: String = config.getString("elasticsearch.port")

    val esConfig: scala.collection.mutable.Map[String, String] =
      scala.collection.mutable.Map(
        "es.nodes" -> s"$esHost:$esPort/",
        "es.nodes.wan.only" -> "true"
      )

    ds.write.format("org.elasticsearch.spark.sql")
      .mode(SaveMode.Append)
      .options(esConfig)
      .save(s"$index/$indexType")
  }
}
