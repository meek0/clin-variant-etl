package bio.ferlab.clin.testutils

import bio.ferlab.datalake.commons.config.{Configuration, ConfigurationLoader, SimpleConfiguration, StorageConf}
import bio.ferlab.datalake.commons.file.FileSystemType.{LOCAL, S3}
import pureconfig.generic.auto._


trait WithTestConfig {
  lazy val initConf: SimpleConfiguration = ConfigurationLoader.loadFromResources[SimpleConfiguration]("config/test.conf")
  lazy implicit val conf: SimpleConfiguration = initConf.copy(datalake = initConf.datalake.copy(
    storages = List(
      StorageConf("clin_datalake", this.getClass.getClassLoader.getResource(".").getFile, LOCAL),
      StorageConf("clin_import", this.getClass.getClassLoader.getResource(".").getFile, LOCAL),
      StorageConf("clin_download", "s3a://test", S3),
      StorageConf("raw", this.getClass.getClassLoader.getResource(".").getFile, LOCAL),
      StorageConf("normalized", this.getClass.getClassLoader.getResource(".").getFile, LOCAL)
    )
  ))
}
