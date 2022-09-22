package bio.ferlab.clin.testutils

import bio.ferlab.datalake.commons.config.{Configuration, ConfigurationLoader, SimpleConfiguration, StorageConf}
import bio.ferlab.datalake.commons.file.FileSystemType.LOCAL
import pureconfig.generic.auto._
import pureconfig.module.enum._

trait WithTestConfig {
  lazy val initConf: SimpleConfiguration = ConfigurationLoader.loadFromResources[SimpleConfiguration]("config/test.conf")
  lazy implicit val conf: Configuration = initConf.copy(datalake = initConf.datalake.copy(
    storages = List(
      StorageConf("clin_datalake", this.getClass.getClassLoader.getResource(".").getFile, LOCAL),
      StorageConf("clin_import", this.getClass.getClassLoader.getResource(".").getFile, LOCAL)
    )
  ))
}
