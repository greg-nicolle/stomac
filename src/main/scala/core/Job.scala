package core

import com.typesafe.config.{Config, ConfigFactory}
import core.utils.transform
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

abstract case class Job[T](repository: Repository[T]) extends App with transform {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  private val config = ConfigFactory.load

  logger.info("Create sparkSession")
  val session: SparkSession = SparkSession.builder()
    .config("spark.driver.host", "localhost")
    .config("spark.ui.enabled", "false")
    .config("spark.memory.fraction", "0.2")
    .master("local[*]")
    .getOrCreate()

  logger.info("Create sparkContext")
  val sc: SparkContext = session.sparkContext

  logger.info("Run before job")
  before(config)
  repository.before(config)
  Try {
    logger.info("Run job")
    repository.save(run(config), config)
  } match {
    case Success(_) =>
      logger.info("Job complete run after job")
      after(config)
      repository.after(config)
    case Failure(err) =>
      logger.error("JobFailure do rollback")
      rollBack(config)
      repository.rollback(config)
      logger.error("Rollback complete", err)
  }

  def before(config: Config): Unit = {}

  def run(config: Config): Dataset[T]

  def after(config: Config): Unit = {}

  def rollBack(config: Config): Unit = {}

}
