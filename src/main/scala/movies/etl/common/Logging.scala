package movies.etl.common

import org.apache.logging.log4j.{LogManager, Logger}

/** logging trait. */
trait Logging {

  /** The logger istance. */
  def logger: Logger = LogManager.getLogger(getClass.getName.stripSuffix("$"))

}
