package job

import com.typesafe.config.Config
import core.Job
import core.repository.ElasticsearchRepository
import core.utils.DatasetParser
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

object WikiVisitsJob
  extends Job(ElasticsearchRepository[WikiDataPageViews]("wikiviews", "type")) {

  override def run(config: Config): Dataset[WikiDataPageViews] = {
    import session.implicits._

    session.read.format("com.databricks.spark.csv")
      .option("delimiter", " ")
      .load(config.getString("wikilog.pageviews"))
      .groupBy("_c0", "_c1").agg(sum($"_c2".cast("Long")).as("_c2"))
      .parseTo[WikiDataPageViews]
      .filter(_.language == "fr")
      .filter(!_.text.contains("Wikipédia"))
      .filter(!_.text.contains("Spécial"))
      .map(x => x.copy(text = x.text.replaceAll("_", " ")))
  }
}

case class WikiDataPageViews(@DatasetParser("_c0") language: String,
                             @DatasetParser("_c1") page: String,
                             @DatasetParser("_c1") text: String,
                             @DatasetParser("_c2") sum: Option[Long])
