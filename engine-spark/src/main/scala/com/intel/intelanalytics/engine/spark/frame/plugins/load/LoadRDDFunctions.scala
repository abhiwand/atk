package com.intel.intelanalytics.engine.spark.frame.plugins.load

import com.intel.intelanalytics.domain.frame.load.{ LineParser, LineParserArguments }
import com.intel.intelanalytics.domain.schema.{ DataTypes, Schema, Column, SchemaUtil }
import com.intel.intelanalytics.engine.spark.SparkEngineConfig
import com.intel.intelanalytics.engine.spark.frame._
import org.apache.spark.{ sql, SparkContext }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.catalyst.expressions.GenericRow

import scala.reflect.ClassTag

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

    if (parser != null) {

      // parse a sample so we can bail early if needed
      parseSampleOfData(fileContentRdd, parser)

      // re-parse the entire file
      parse(fileContentRdd, parser)
    }
    else {
      val listColumn = List(Column("data_lines", DataTypes.str))
      val rows = fileContentRdd.map(s => new GenericRow(Array[Any](s)).asInstanceOf[sql.Row])
      ParseResultRddWrapper(new FrameRDD(new Schema(listColumn), rows).toLegacyFrameRDD, null)
    }

  }

  /**
   * Load each line from client data into an RDD of Row objects.
   * @param sc SparkContext used for textFile reading
   * @param data data to parse
   * @param parser parser provided
   * @return  RDD of Row objects
   */
  def loadAndParseData(sc: SparkContext,
                       data: List[List[Any]],
                       parser: LineParser): ParseResultRddWrapper = {

    //val dataContentRDD: RDD[String] = sc.parallelize(data).map(s => s.mkString(","))
    val dataContentRDD: RDD[Any] = sc.parallelize(data)
    // parse a sample so we can bail early if needed
    parseSampleOfData(dataContentRDD, parser)

    // re-parse the entire file
    parse(dataContentRDD, parser)
  }

  /**
   * Parse a sample of the file so we can bail early if a certain threshold fails.
   *
   * Throw an exception if too many rows can't be parsed.
   *
   * @param fileContentRdd the rows that need to be parsed (the file content)
   * @param parser the parser to use
   */
  private[frame] def parseSampleOfData[T: ClassTag](fileContentRdd: RDD[T],
                                                    parser: LineParser): Unit = {

    //parse the first number of lines specified as sample size and make sure the file is acceptable
    val sampleSize = SparkEngineConfig.frameLoadTestSampleSize
    val threshold = SparkEngineConfig.frameLoadTestFailThresholdPercentage

    val sampleRdd = MiscFrameFunctions.getPagedRdd[T](fileContentRdd, 0, sampleSize, sampleSize)

    //cache the RDD since it will be used multiple times
    sampleRdd.cache()

    val preEvaluateResults = parse(sampleRdd, parser)
    val failedCount = preEvaluateResults.errorLines.count()
    val sampleRowsCount: Long = sampleRdd.count()

    val failedRatio: Long = if (sampleRowsCount == 0) 0 else 100 * failedCount / sampleRowsCount

    //don't need it anymore
    sampleRdd.unpersist()

    if (failedRatio >= threshold) {
      val errorExampleRecord = preEvaluateResults.errorLines.first().clone()
      val errorRow = errorExampleRecord { 0 }
      val errorMessage = errorExampleRecord { 1 }
      throw new Exception(s"Parse failed on $failedCount rows out of the first $sampleRowsCount, " +
        s" please ensure your schema is correct.\nExample record that parser failed on : $errorRow    " +
        s" \n$errorMessage")
    }
  }

  /**
   * Parse rows and separate into successes and failures
   * @param rowsToParse the rows that need to be parsed (the file content)
   * @param parser the parser to use
   * @return the parse result - successes and failures
   */
  private[frame] def parse[T](rowsToParse: RDD[T], parser: LineParser): ParseResultRddWrapper = {

    val schemaArgs = parser.arguments.schema
    val skipRows = parser.arguments.skip_rows
    val parserFunction = getLineParser(parser, schemaArgs.columns.map(_._2).toArray)

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
      new ParseResultRddWrapper(new LegacyFrameRDD(schema.schema, successesRdd), new LegacyFrameRDD(SchemaUtil.ErrorFrameSchema, failuresRdd))
    }
    finally {
      parseResultRdd.unpersist(blocking = false)
    }
  }

  private[frame] def getLineParser[T](parser: LineParser, columnTypes: Array[DataTypes.DataType]): T => RowParseResult = {
    parser.name match {
      //TODO: look functions up in a table rather than switching on names
      case "builtin/line/separator" => {
        val args = parser.arguments match {
          //TODO: genericize this argument conversion
          case a: LineParserArguments => a
          case x => throw new IllegalArgumentException(
            "Could not convert instance of " + x.getClass.getName + " to  arguments for builtin/line/separator")
        }
        val rowParser = new CsvRowParser(args.separator, columnTypes)
        s => rowParser(s.asInstanceOf[String])
      }
      case "builtin/upload" => {
        val uploadParser = new UploadParser(columnTypes)
        row => uploadParser(row.asInstanceOf[List[Any]])
      }
      case x => throw new Exception("Unsupported parser: " + x)
    }
  }
}
