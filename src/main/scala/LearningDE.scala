import job.MySparkJob
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import utils.{Constants, DatabricksUtils, EMountMethod}

object LearningDE {
  val storageAccountName: String = DatabricksUtils.getKeyVaultSecret(Constants.storageAccountKeyName)
  val containerName: String = DatabricksUtils.getKeyVaultSecret(Constants.containerKeyName)
  private val newContainerName: String = DatabricksUtils.getKeyVaultSecret(Constants.newContainerKeyName)
  val appID: String = DatabricksUtils.getKeyVaultSecret(Constants.appIDKeyName)
  val secretID: String = DatabricksUtils.getKeyVaultSecret(Constants.secretKeyName)
  val directoryID: String = DatabricksUtils.getKeyVaultSecret(Constants.directoryIDKeyName)
  val sasToken: String = DatabricksUtils.getKeyVaultSecret(Constants.sasTokenKeyName)
//  val dwServer: String = DatabricksUtils.getKeyVaultSecret(Constants.dwServerKeyName)
//  val dwDatabase: String = DatabricksUtils.getKeyVaultSecret(Constants.dwDatabaseKeyName)
//  val dwUser: String = DatabricksUtils.getKeyVaultSecret(Constants.dwUserKeyName)
//  val dwPass: String = DatabricksUtils.getKeyVaultSecret(Constants.dwPassKeyName)
//  private val tempDirPath: String = DatabricksUtils.getKeyVaultSecret(Constants.tempDirKeyName)
//  private val storageAccountAccessKey: String = DatabricksUtils.getKeyVaultSecret(Constants.storageAccountAccessKeyName)


  private def initializeSparkSession(): SparkSession = {
    println("Initial session - Get/Create Spark session and Spark context")
    val spark = SparkSession.builder().getOrCreate()
    val sc = SparkContext.getOrCreate()
    sc.setLogLevel("ERROR")
    println("Initial session - Ending")
    spark
  }

  private def completed(): Unit = {
    DatabricksUtils.unMountPoint(Constants.mountPoint)
    DatabricksUtils.unMountPoint(Constants.newMountPoint)
    println("Complete the Spark Job!")
  }

  private def mountADLS(spark: SparkSession, containerName: String, mountMethod: String, mountPoint: String = Constants.mountPoint): Unit = {
    mountMethod match {
      case EMountMethod.SAS =>
        DatabricksUtils.mountingWithSASToken(
          containerName,
          storageAccountName = storageAccountName,
          sasToken = sasToken,
          mountPoint = mountPoint,
        )
        sparkConfSetUsingSAS(spark, sasToken)
        println(s"Mount the ADLS Gen2 using ${EMountMethod.SAS}")
      case _ =>
        DatabricksUtils.mountingWithAzureCredentials(
          containerName,
          storageAccountName = storageAccountName,
          appID = appID,
          secretID = secretID,
          directoryID = directoryID,
          mountPoint = mountPoint,
        )
        sparkConfSetUsingCredentials(spark)
        println(s"Mount the ADLS Gen2 using ${EMountMethod.Credentials}")
    }
    println(s"Mount point path: $mountPoint")
  }

  private def sparkConfSetUsingCredentials(spark: SparkSession): Unit = {
    val endPoint = "https://login.microsoftonline.com/" + directoryID + "/oauth2/token"
    spark.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
    spark.conf.set("dfs.adls.oauth2.client.id", appID)
    spark.conf.set("dfs.adls.oauth2.credential", secretID)
    spark.conf.set("dfs.adls.oauth2.refresh.url", endPoint)
  }

  private def sparkConfSetUsingSAS(spark: SparkSession, sasToken: String): Unit = {
    spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net", "SAS")
    spark.conf.set("fs.azure.sas.token.provider.type.<storage-account>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
    spark.conf.set(s"fs.azure.sas.fixed.token.$storageAccountName.dfs.core.windows.net", sasToken)
  }

  def main(args: Array[String]): Unit = {
    try {
      val mountMethod = args.mkString
      val spark = initializeSparkSession()
      if (spark != null) {
        println("The Spark have been successfully initialised")
      } else {
        println("Failed to initialised Spark session")
      }
      println("Starting - Mount the ADLS Gen2 to path")
      mountADLS(spark, containerName, mountMethod)
      println("Ending - Mount the ADLS Gen2 to path")
      println("Starting the Spark Job!")
      println("Starting - Read parquet file from the container and print the schema")
      val parquetFileDF = MySparkJob.readParquetFile(spark, Constants.mountPoint)
      parquetFileDF.printSchema()
      println("Ending - Read parquet file from the container and print the schema")
      println("Starting - Validate if real data schema in the parquet files match Expected schema")
      println("Expected schema")
      // Create a expected schema DataFrame
      val expectedSchemaDF = MySparkJob.createExpectedSchemaDF(spark)
      expectedSchemaDF.printSchema()
      MySparkJob.matchSchema(parquetFileDF, expectedSchemaDF)
      println("Ending - Validate if real data schema in the parquet files match Expected schema")
      println("Starting - Renamed the column name cc to cc_mod")
      val renamedDF = MySparkJob.renameColumnName(dataFrame = parquetFileDF)
      println(renamedDF.printSchema())
      println("Ending - Renamed the column name cc to cc_mod")
      println("Starting - Add tag to metadata of new column name")
      val addTagDF = MySparkJob.addTagToRenamedColumnDF(dataFrame = renamedDF)
      addTagDF.schema.foreach(t => println(s"${t.name}: metadata = ${t.metadata}"))
      println("Ending - Add tag to metadata of new column name")
      println("Starting - Remove duplicate rows on DataFrame")
      val removeDupRowsDF = MySparkJob.removeDuplicatedRows(dataFrame = addTagDF)
      println("Before remove duplicate rows, rows length = " + addTagDF.schema.fields.length)
      println("After remove duplicate rows, rows length = " + removeDupRowsDF.schema.fields.length)
      println("Ending - Remove duplicate rows on DataFrame")

      println("Starting - Transform to new container in ADLS Gen2")
      mountADLS(
        spark = spark,
        containerName = newContainerName,
        mountMethod = mountMethod,
        mountPoint = Constants.newMountPoint
      )
      MySparkJob.transformToNewContainer(removeDupRowsDF)
      println("Ending - Transform to new container in ADLS Gen2")

      // Use for Azure Synapse Analytics

      //      println("Starting - Transform to Data Warehouse")
      //      val sc = SparkContext.getOrCreate()
      //      sc.hadoopConfiguration.set(s"fs.azure.account.key.$storageAccountName.dfs.core.windows.net", storageAccountAccessKey)
      //      spark.conf.set("spark.sql.parquet.writeLegacyFormat", "true")
      //      val tempDir = s"abfss://$newContainerName@$storageAccountName.dfs.core.windows.net/$tempDirPath"
      //      MySparkJob.transformToDWTable(removeDupRowsDF, tempDir, dwDatabase, dwServer, dwUser, dwPass)
      //      println("Ending - Transform to Data Warehouse")

    } finally {
      completed()
    }
  }
}
