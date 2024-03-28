package bio.ferlab.clin.etl

import org.apache.commons.lang3.StringUtils

package object es {

  def sanitizeArg(arg: String): Option[String] = {
    Option(arg).map(s => s.replace("\"", "")).filter(s => StringUtils.isNotBlank(s))
  }
}
