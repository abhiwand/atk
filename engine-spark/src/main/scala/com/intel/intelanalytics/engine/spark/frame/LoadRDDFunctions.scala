package com.intel.intelanalytics.engine.spark.frame

import org.apache.spark.SparkContext
import com.intel.intelanalytics.domain.frame.load.{ LineParserArguments, LineParser }
import com.intel.intelanalytics.engine.spark.{ SparkOps, SparkEngineConfig }
import org.apache.spark.rdd.RDD
import com.intel.intelanalytics.domain.schema.{ DataTypes, SchemaUtil }

/**
 * Helper functions for loading an RDD
 */
object LoadRDDFunctions extends Serializable {

  /**
   * Load each line from CSV file into an RDD of Row objects.
   * @param sc SparkContext used for textFile reading
   * @param fileName name of file to parse
   * @param parser
   * @return  RDD of Row objects
   */
  def loadAndParseLines(sc: SparkContext,
                        fileName: String,
                        parser: LineParser,
                        partitions: Int): ParseResultRddWrapper = {

    val fileContentRdd: RDD[String] = sc.textFile(fileName, partitions).filter(_.trim() != "")

    // parse a sample so we can bail early if needed
    parseSampleOfFile(fileContentRdd, parser)

    // re-parse the entire file
    parse(fileContentRdd, parser)
  }

  /**
   * Parse a sample of the file so we can bail early if a certain threshold fails.
   *
   * Throw an exception if too many rows can't be parsed.
   *
   * @param fileContentRdd the rows that need to be parsed (the file content)
   * @param parser the parser to use
   */
  private[frame] def parseSampleOfFile(fileContentRdd: RDD[String],
                                       parser: LineParser): Unit = {

    //parse the first number of lines specified as sample size and make sure the file is acceptable
    val sampleSize = SparkEngineConfig.frameLoadTestSampleSize
    val threshold = SparkEngineConfig.frameLoadTestFailThresholdPercentage

    val sampleRdd = SparkOps.getPagedRdd(fileContentRdd, 0, sampleSize, sampleSize)

    //cache the RDD since it will be used multiple times
    sampleRdd.cache()

    val preEvaluateResults = parse(sampleRdd, parser)
    val failedCount = preEvaluateResults.errorLines.count()
    val sampleRowsCount: Long = sampleRdd.count()

    val failedRatio: Long = if (sampleRowsCount == 0) 0 else 100 * failedCount / sampleRowsCount

    //don't need it anymore
    sampleRdd.unpersist()

    if (failedRatio >= threshold)
      throw new Exception(s"Parse failed on $failedCount rows out of the first $sampleRowsCount, " +
        " please ensure your schema is correct")
  }

  /**
   * Parse rows and separate into successes and failures
   * @param rowsToParse the rows that need to be parsed (the file content)
   * @param parser the parser to use
   * @return the parse result - successes and failures
   */
  private[frame] def parse(rowsToParse: RDD[String], parser: LineParser): ParseResultRddWrapper = {

    val schema = parser.arguments.schema
    val skipRows = parser.arguments.skip_rows
    val parserFunction = getLineParser(parser, schema.columns.map(_._2).toArray)

    val parseResultRdd = rowsToParse.mapPartitionsWithIndex {
      case (partition, lines) => {
        if (partition == 0) {
          lines.drop(skipRows.getOrElse(0)).map(parserFunction)
        }
        else {
          lines.map(parserFunction)
        }
      }
    }
    try {
      parseResultRdd.cache()
      val successesRdd = parseResultRdd.filter(rowParseResult => rowParseResult.parseSuccess)
        .map(rowParseResult => rowParseResult.row)
      val failuresRdd = parseResultRdd.filter(rowParseResult => !rowParseResult.parseSuccess)
        .map(rowParseResult => rowParseResult.row)

      val schema = parser.arguments.schema
      new ParseResultRddWrapper(new FrameRDD(schema, successesRdd), new FrameRDD(SchemaUtil.ErrorFrameSchema, failuresRdd))
    }
    finally {
      parseResultRdd.unpersist(blocking = false)
    }
  }

  private[frame] def getLineParser(parser: LineParser, columnTypes: Array[DataTypes.DataType]): String => RowParseResult = {
    parser.name match {
      //TODO: look functions up in a table rather than switching on names
      case "builtin/line/separator" => {
        val args = parser.arguments match {
          //TODO: genericize this argument conversion
          case a: LineParserArguments => a
          case x => throw new IllegalArgumentException(
            "Could not convert instance of " + x.getClass.getName + " to  arguments for builtin/line/separator")
        }

        val rowParser = new RowParser(args.separator, columnTypes)
        s => rowParser(s)

      }
      case x => throw new Exception("Unsupported parser: " + x)
    }
  }
}
