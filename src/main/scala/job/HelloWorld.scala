package job

import com.typesafe.config.Config
import core.Job
import core.repository.ShowRepository
import org.apache.spark.sql.Dataset

object HelloWorld extends Job[HelloWorldDS](ShowRepository[HelloWorldDS]()) {

  override def before(config: Config): Unit = {
    logger.info("before")
  }

  override def rollBack(config: Config): Unit = logger.info("rollback !!")

  override def run(config: Config): Dataset[HelloWorldDS] = {
    logger.info("run")

    import session.implicits._

    Seq(
      HelloWorldDS("Hello"),
      HelloWorldDS("world"),
      HelloWorldDS("!")
    ).toDS
  }
}

case class HelloWorldDS(data: String)