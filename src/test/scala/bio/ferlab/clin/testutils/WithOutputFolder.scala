package bio.ferlab.clin.testutils

import java.nio.file.{Files, Path}

import org.apache.commons.io.FileUtils

trait WithOutputFolder {
  def withOutputFolder[T](prefix: String)(block: String => T): T = {
    val output: Path = Files.createTempDirectory(prefix)
    try {
      block(output.toAbsolutePath.toString)
    } finally {
      FileUtils.deleteDirectory(output.toFile)
    }
  }
}
