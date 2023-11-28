package job

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import utils.Constants

object MySparkJob {
  /**
   * Read the parquet file from the ADLS Gen2 mounted to path
   */
  def readParquetFile(spark: SparkSession, mountPointPath: String): DataFrame = {
    spark.read.parquet(mountPointPath)
  }

  /**
   * Create the expected schema DataFrame by empty DF & StructType
   */
  def createExpectedSchemaDF(spark: SparkSession): DataFrame = {
    val expectedSchemeString = "registration_dttm,id,first_name,last_name,email,gender,ip_address,cc,country,birthdate,salary,title,comments"
    val expectedSchemaFields = expectedSchemeString.split(",")
      .map {
        case field@"registration_dttm" => StructField(field, TimestampType, nullable = true)
        case field@"id" => StructField(field, IntegerType, nullable = true)
        case field@"salary" => StructField(field, DoubleType, nullable = true)
        case field => StructField(field, StringType, nullable = true)
      }
    val expectedSchema = StructType(expectedSchemaFields)
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], expectedSchema)
  }

  /** Function validate the schema of parquet files (target) with Expected schema by
   * validating the fieldName, dataType and nullable
   */
  def matchSchema(targetDF: DataFrame, expectedDF: DataFrame): Unit = {
    val expectedTuple = expectedDF.schema.fields.map(e => (e.name, e.dataType))
    val targetTuple = targetDF.schema.fields.map(t => (t.name, t.dataType))
    val isTheSameLength = expectedTuple.length == targetTuple.length
    val isTheSameFields = expectedTuple.diff(targetTuple).isEmpty
    if (isTheSameFields && isTheSameLength) {
      println("Match")
    } else {
      println("Mismatch")
    }
  }

  /**
   * Rename the parquet DataFrame's column name from cc to cc_mod
   */
  def renameColumnName(dataFrame: DataFrame): DataFrame = {
    dataFrame.withColumnRenamed(Constants.columnName, Constants.newColumnName)
  }

  /**
   * Add a tag “this column has been modified” to metadata of cc_mod column
   */
  def addTagToRenamedColumnDF(dataFrame: DataFrame): DataFrame = {
    val metadataBuilder = new MetadataBuilder().putString("tag", Constants.tag).build()
    val newColumnValue = dataFrame.col(Constants.newColumnName).as(Constants.columnName, metadataBuilder)
    dataFrame.withColumn(Constants.newColumnName, newColumnValue)
  }

  /**
   * Remove duplicate rows on DataFrame
   */
  def removeDuplicatedRows(dataFrame: DataFrame): DataFrame = {
    dataFrame.distinct()
  }

  /**
   * Transform to Data Warehouse (Azure Synapse Analytics)
   */
  def transformToDWTable(dataFrame: DataFrame, tempDir: String, dwDatabase: String,
                         dwServer: String, dwUser: String, dwPass: String): Unit = {
    val dwJdbcPort = "1433"
    val sqlDWUrlFooter = "encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30;"
    val sqlDwUrl = s"jdbc:sqlserver://$dwServer:$dwJdbcPort;database=$dwDatabase;user=$dwUser;password=$dwPass;$sqlDWUrlFooter"
    dataFrame.write
      .format("com.databricks.spark.sqldw")
      .option("url", sqlDwUrl)
      .option("dbTable", "VinhCreatedTable")
      .option("forwardSparkAzureStorageCredentials", "true")
      .option("tempDir", tempDir)
      .mode("overwrite")
      .save()
  }
}
