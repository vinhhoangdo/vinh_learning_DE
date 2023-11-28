package utils

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils

object DatabricksUtils {
  /**
   * Retrieve a secret from Azure Key Vault using Databricks Utils API
   */
  def getKeyVaultSecret(keyName: String): String = {
    dbutils.secrets.get(scope = Constants.scopeName, key = keyName)
  }

  /**
   * Mount an Azure Data Lake Storage Gen2 container using SAS Token
   */
  def mountingWithSASToken(containerName: String, storageAccountName: String, sasToken: String, mountPoint: String): Boolean = {
    val source = "wasbs://" + containerName + "@" + storageAccountName + ".blob.core.windows.net/"
    val configs = "fs.azure.sas." + containerName + "." + storageAccountName + ".blob.core.windows.net"
    dbutils.fs.mount(
      source = source,
      mountPoint = mountPoint,
      extraConfigs = Map(configs -> sasToken)
    )
  }

  /**
   * Mount an Azure Data Lake Storage Gen2 container using Microsoft Entra ID
   */
  def mountingWithAzureCredentials(containerName: String, storageAccountName: String,
                                   appID: String, directoryID: String,
                                   secretID: String, mountPoint: String): Boolean = {
    val endPoint = "https://login.microsoftonline.com/" + directoryID + "/oauth2/token"
    val source = "abfss://" + containerName + "@" + storageAccountName + ".dfs.core.windows.net/"
    val configs = Map(
      "fs.azure.account.auth.type" -> "OAuth",
      "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
      "fs.azure.account.oauth2.client.id" -> appID,
      "fs.azure.account.oauth2.client.secret" -> secretID,
      "fs.azure.account.oauth2.client.endpoint" -> endPoint,
    )

    dbutils.fs.mount(
      source = source,
      mountPoint = mountPoint,
      extraConfigs = configs
    )
  }
  /**
   * Unmount a mount point
   */
  def unMountPoint(mountPointPath: String): Unit = {
    dbutils.fs.unmount(mountPointPath)
  }

  def retrieveMountPointPath(mountPointPath: String): String = {
    val isHasMountPoint = dbutils.fs.mounts().exists(mount => mount.mountPoint == mountPointPath)
    if(isHasMountPoint) {
      dbutils.fs.ls(mountPointPath).head.path
    } else {
      println(s"$mountPointPath doesn't exist")
      ""
    }
  }
}
