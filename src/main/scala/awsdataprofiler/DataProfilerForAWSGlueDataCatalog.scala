/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package awsdataprofiler

import java.time.{OffsetDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util

import awsservices.Glue
import com.amazon.deequ.analyzers.runners.AnalyzerContext.successMetricsAsDataFrame
import com.amazon.deequ.analyzers.runners.{AnalysisRunBuilder, AnalysisRunner}
import com.amazon.deequ.analyzers._
import org.apache.log4j.{Logger, PropertyConfigurator}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType, TimestampType}

import collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.util.matching.Regex


object DataProfilerForAWSGlueDataCatalog extends App {

	lazy val logger = Logger.getLogger(DataProfilerForAWSGlueDataCatalog.getClass)

	lazy val appName: String = "data-profiler-for-aws-glue-data-catalog"

	//Input Parameter - String - Database Name - Required
	var dbName: String = ""

	//Input Parameter - String - AWS Region - Required
	var region: String = ""

	//Input Parameter - Boolean - Compute expensive statistics - Default: false
	var compExp: Boolean = false

	//Input Parameter - String - Statistics Prefix - Default: DQP
	var statsPrefix: String = "DQP"

	//Input Parameter - s3BucketPrefix - S3 bucket/prefix to save csv files of statistics - Default: blank
	var s3BucketPrefix: String = ""

	//Input Parameter - When true convert boolean, date and timestamp columns to string so that they can profiled
	//as Deequ does not support these column types - Default: false
	var profileUnsupportedTypes: Boolean = false

	//Input Parameter - noOfBins - for String columns when compExp=true - Default: 10
	var noOfBins: Int = 10

	//Input Parameter - quantiles - for Numeric columns number of quantiles for ApproxQuantiles - Default: 10
	var quantiles: Int = 10

	// If set to true code is executed with a local Spark Session - Useful for Dev and Test
	lazy val executeLocal: Boolean = false

	// Execution date time, used to identify execution, can also be used as execution id
	lazy val run_dt = OffsetDateTime.now(ZoneOffset.UTC)

	override def main(args: Array[String]): Unit = {

		PropertyConfigurator.configure(getClass.getClassLoader.getResourceAsStream("log4j.xml"))

		val parser = new scopt.OptionParser[CommandLineArgs](appName) {

			head(appName, "1.0")

			opt[String]('d', "dbName")
			  .required()
			  .valueName("<glue-database-name>")
			  .action((x, c) => c.copy(dbName = x))
			  .text("Required (String): AWS Glue Database Name")

			opt[String]('r', "region")
			  .required()
			  .valueName("<aws-region>")
			  .action((x, c) => c.copy(region = x))
			  .text("Required (String): AWS Region - https://docs.aws.amazon.com/general/latest/gr/rande.html#regional-endpoints")

			opt[Boolean]('c', "compExp")
			  .valueName("<true | false>")
			  .action((x, c) => c.copy(compExp = x))
			  .text("Optional (Boolean) - Default = false: If set to true complex statistics are calculated.")

			opt[String]('p', "statsPrefix")
			  .action((x, c) => c.copy(statsPrefix = x) )
			  .text("Optional (String) - Default = 'DQP': Prefix for statistics names.")

			opt[String]('s', "s3BucketPrefix")
			  .action((x, c) => c.copy(s3BucketPrefix = x) )
			  .text("Optional (String) - Default = '': S3 bucket/prefix to save parquet files of statistics format: " +
				"' my_bucket[/my_prefix]'.")

			opt[Boolean]('u', "profileUnsupportedTypes")
			  .valueName("<true | false>")
			  .action((x, c) => c.copy(profileUnsupportedTypes = x) )
			  //.text("Optional (Boolean) - Default = false: When true convert boolean, date and timestamp columns to " +
			  .text("Optional (Boolean) - Default = false: When true convert boolean and date columns to " +
				"string so that they can profiled as Deequ does not support these column types.")

			opt[Int]('b', "noOfBins")
			  .action((x, c) => c.copy(noOfBins = x) )
			  .text("Optional (Int) - Default = 10: When compExp = true, sets the number of maximum values to create " +
				"for the Histogram analyzer for String columns.")

			opt[Int]('q', "quantiles")
			  .action((x, c) => c.copy(quantiles = x) )
			  .text("Optional (Int) - Default = 10: for numeric columns, sets the number of Quantiles to be calculated for the " +
				"ApproxQuantiles analyzer.")
		}

		parser.parse(args, CommandLineArgs()) match {
			case Some(config) => {
				dbName = config.dbName
				region = config.region
				compExp = config.compExp
				statsPrefix = config.statsPrefix+"__"
				s3BucketPrefix = config.s3BucketPrefix
				profileUnsupportedTypes = config.profileUnsupportedTypes
				noOfBins = config.noOfBins
				quantiles = config.quantiles
				;
			}
			case None => {
				//Display help message
				System.exit(1)
			}
		}

		if(logger.isInfoEnabled){
			logger.info(appName+" --- Input parameters - START")
			logger.info("\t dbName                  : "+dbName)
			logger.info("\t region                  : "+region)
			logger.info("\t compExp                 : "+compExp)
			logger.info("\t statsPrefix             : "+statsPrefix)
			logger.info("\t s3BucketPrefix          : "+s3BucketPrefix)
			logger.info("\t profileUnsupportedTypes : "+profileUnsupportedTypes)
			logger.info("\t noOfBins                : "+noOfBins)
			logger.info("\t quantiles               : "+quantiles)
			logger.info(appName+" --- Input parameters - END")
			logger.info(appName+" --- AWS Java JDK Version: "+com.amazonaws.util.VersionInfoUtils.getVersion)
		}

		runProfiler()
	}


	def runProfiler(): Unit = {

		val dfs = new util.HashMap[String, DataFrame]()

		if(!executeLocal){

			val tables = Glue.getDBTables(region, dbName).asScala

			tables.foreach(t => {
				val tableName = t.getName
				dfs.put(tableName, computeProfilerStats(tableName))
			})
		}
		else {
			val spark: SparkSession = SparkSession.builder()
			  .master("local[4]")
			  .appName("DQ_")
			  .config("spark.driver.host", "localhost")
			  .config("spark.ui.enabled", "false")
			  .enableHiveSupport()
			  .getOrCreate()

			spark.sparkContext.setCheckpointDir(System.getProperty("java.io.tmpdir"))

			val tables = spark.catalog
			  .listTables(dbName)
			  .select("name")

			tables.collect.foreach(row => {
				val tn = row.getString(0)
				dfs.put(tn, computeProfilerStats(tn))
			})
		}

		dfs.foreach(kv => {

			val tableName = kv._1
			val metricsDf = kv._2

			val tableParams: util.HashMap[String, String] = new util.HashMap[String, String]()
			val columnsParams: util.HashMap[String, util.HashMap[String, String]] = new  util.HashMap[String, util.HashMap[String, String]]()

			val tableDf = metricsDf.select("name","value").where("Entity = 'Dataset'")
			val colsDf = metricsDf.select("instance","name","value").where("Entity = 'Column'")
			val colNamesDf = colsDf.select("instance").dropDuplicates()

			tableDf.collect().foreach(row => {
				tableParams.put(statsPrefix+row.getAs("name").toString, row.getAs("value").toString)
			})

			colNamesDf.collect().foreach(row => {
				val colName = row.getAs("instance").toString

				val colStatsDf = colsDf.select("name","value").where("instance = '"+colName+"'")

				val colStats = new util.HashMap[String, String]()
				colStatsDf.collect().foreach(row => {
					colStats.put(statsPrefix+row.getAs("name").toString, row.getAs("value").toString)
				})

				columnsParams.put(colName, colStats)
			})

			if(!executeLocal){
				Glue.updateTableMetadata(region, dbName, tableName, statsPrefix, tableParams, columnsParams)
			}

			// Write to S3 if a bucket was specified in the input parameters
			if (!s3BucketPrefix.equals("")){

				val en_metricsDf = metricsDf
				  .withColumn("db_name_embed", lit(dbName))
				  .withColumn("table_name_embed", lit(tableName))
				  .withColumn("profiler_run_dt",
					  lit(DateTimeFormatter.ofPattern("yyyy-MM-dd").format(run_dt))
						.cast(DateType))
				  .withColumn("profiler_run_ts",
					  lit(DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss").format(run_dt))
						.cast(TimestampType))

				var s3Schema = "s3"
				if(executeLocal) s3Schema ="s3a"

				en_metricsDf
				  .coalesce(1)
				  .write
				  .mode("append")
				  .parquet(s3Schema+"://"+s3BucketPrefix+"/db_name="+dbName+"/table_name="+tableName)
			}
		})
	}


	def computeProfilerStats(tableName: String): DataFrame = {

		val spark: SparkSession = SparkSession.builder()
		  .enableHiveSupport()
		  .getOrCreate()

		var df = spark.sqlContext.table(dbName + "." + tableName)

		if(logger.isInfoEnabled){
			logger.info(s" -=-=-=-=- Processing Table ${dbName}.${tableName} -=-=-=-=- ")
			df.printSchema
			df.schema.fields.foreach(f => {
				logger.info(s"${f.name} - ${f.dataType} - ${f.dataType.catalogString}")
			})
		}

		val analysisRunner: AnalysisRunBuilder = AnalysisRunner.onData(df)
		analysisRunner.addAnalyzer(Size())

		val colNameTypeDF = getSchemaDF(df)

		if(profileUnsupportedTypes){
			df = convert_unsupported_types_to_string(df)
		}

		df.schema.fields.foreach(f => {

			val t = f.dataType.catalogString

			if(isString(t)){
				addTextAnalyzers(f.name, analysisRunner, compExp)
			}
			else if (isNumeric(t)){
				addNumericAnalyzers(f.name, analysisRunner, compExp)
			}

		})

		val analysisResult = analysisRunner.run()

		var metrics = successMetricsAsDataFrame(spark, analysisResult)

		metrics = metrics.join(colNameTypeDF
		  .withColumnRenamed("name","instance"),
			Seq("instance"),
			"left"
		)

		if(logger.isInfoEnabled){
			logger.info(analysisResult)
			metrics.sort("instance", "name").show(100, false)
		}

		metrics
	}


	def addTextAnalyzers(colName: String, analysisRunner: AnalysisRunBuilder, computeExpensive: Boolean) = {

		analysisRunner.addAnalyzer(ApproxCountDistinct(colName))
		  .addAnalyzer(Completeness(colName))
		  .addAnalyzer(Distinctness(colName))
		  .addAnalyzer(MaxLength(colName))
		  .addAnalyzer(MinLength(colName))

		if(computeExpensive){
			analysisRunner.addAnalyzer(CountDistinct(colName))
			  .addAnalyzer(Entropy(colName))
			  .addAnalyzer(Histogram(colName, null, noOfBins))
			  .addAnalyzer(UniqueValueRatio(colName))
			  .addAnalyzer(Uniqueness(colName))
		}

	}


	def addNumericAnalyzers(colName: String, analysisRunner: AnalysisRunBuilder, computeExpensive: Boolean) = {

		val percentiles = (1 to quantiles).map {
			_.toDouble / quantiles
		}

		analysisRunner.addAnalyzer(ApproxCountDistinct(colName))
		  .addAnalyzer(ApproxQuantiles(colName, percentiles))
		  .addAnalyzer(Completeness(colName))
		  .addAnalyzer(Distinctness(colName))
		  .addAnalyzer(Maximum(colName))
		  .addAnalyzer(Mean(colName))
		  .addAnalyzer(Minimum(colName))
		  .addAnalyzer(StandardDeviation(colName))
		  .addAnalyzer(Sum(colName))
	}


	def isString(dfType: String): Boolean = {
		val stringTypes = List("string", "varchar", "char")

		stringTypes contains dfType
	}


	def isNumeric(dfType: String): Boolean = {

		val ptrn: Regex = "\\(\\d*,\\d*\\)".r
		val t = ptrn.replaceFirstIn(dfType,"")
		val numTypes = List("tinyint", "smallint", "short", "int", "bigint", "float", "double", "decimal")

		numTypes contains t
	}


	def convert_unsupported_types_to_string(df: DataFrame): DataFrame = {

		var df2 = df

		df.schema.fields.foreach(f => {

			val t = f.dataType.catalogString

			//if (t.equals("boolean") || t.equals("date") || t.equals("timestamp")){
			if (t.equals("boolean") || t.equals("date")){
				df2 = df2.withColumn(f.name, col(f.name).cast(StringType))
				if(logger.isInfoEnabled){
					logger.info(s"Converted column ${f.name} from '${t}' to 'String'")
				}
			}
		})

		df2
	}


	def getSchemaDF(df: DataFrame): DataFrame = {

		val spark: SparkSession = SparkSession.builder()
		  .enableHiveSupport()
		  .getOrCreate()

		val schema = StructType(
			StructField("name", StringType, false) ::
			  StructField("type", StringType, false) :: Nil)

		var ctdf = spark.createDataFrame(
			spark.sparkContext.emptyRDD[Row],
			schema)

		import spark.implicits._
		df.schema.fields.foreach(f => {
			ctdf = ctdf.union(Seq((f.name,f.dataType.catalogString)).toDF())
		})

		ctdf
	}

}
