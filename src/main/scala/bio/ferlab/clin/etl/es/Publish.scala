package bio.ferlab.clin.etl.es

import bio.ferlab.datalake.spark3.elasticsearch.ElasticSearchClient
import org.apache.http.client.methods.HttpGet
import org.apache.http.util.EntityUtils

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
  
  val previousIndex = findIndexWithAlias(alias) // ex: clin_staging_variant_centric_re_000
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
  
  def findIndexWithAlias(alias: String): Option[String] = {
    val body = EntityUtils.toString(esClient.http.execute(new HttpGet(s"$esUrl/_cat/aliases")).getEntity)
    val aliases = body.split("\n").map(line => line.split("\\s+")).map(e => (e(0), e(1))).toMap
    aliases.get(alias)
  }

}
