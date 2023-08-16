package bio.ferlab.clin.etl.es

import bio.ferlab.datalake.spark3.elasticsearch.ElasticSearchClient

object Publish extends App {

  //local truststore
  //System.setProperty("javax.net.ssl.trustStore", "cacerts.jks")
  //System.setProperty("javax.net.ssl.trustStorePassword", "changeit")

  println(s"ARGS: " + args.mkString("[", ", ", "]"))

  val Array(
  esNodes,          // http://0.0.0.0
  username,         // basic auth username
  password,         // basic auth password
  alias,            // alias to create, ex: clin_staging_variant_centric
  release_id,       // release id, ex: re_001
  ) = args

  val esUrl = esNodes.split(',').head

  implicit val esClient: ElasticSearchClient = new ElasticSearchClient(esUrl, sanitizeArg(username), sanitizeArg(password))

  val previousIndex = retrievePreviousIndex(alias) // ex: clin_staging_variant_centric_re_000
  val newIndex = s"${alias}_$release_id"  //ex: clin_staging_variant_centric_re_001

  if (previousIndex.isDefined) {
    val previousIndexName = previousIndex.get
    if (!previousIndexName.equals(newIndex)) {  // if same name, the alias will be removed after being added
      println(s"Remove alias: $alias <=> $previousIndexName")
      println(s"Add alias: $alias <=> $newIndex")
      esClient.setAlias(List(newIndex), List(previousIndexName), alias)
    }
  } else {
    println(s"Add alias: $alias <=> $newIndex")
    esClient.setAlias(List(newIndex), List(), alias)
  }

  def retrievePreviousIndex(alias: String): Option[String] = {
    val response = esClient.getAliasIndices(alias)
    response.find(_.startsWith(alias))
  }

}
