package bio.ferlab.clin.etl.varsome

import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.apache.http.protocol.HttpContext
import org.apache.http.util.EntityUtils
import org.apache.http.{HttpHeaders, HttpRequest, HttpRequestInterceptor}

case class VarsomeHttpClient(varsomeUrl: String, varsomeToken: String) {

  private val http: CloseableHttpClient = {
    val client = HttpClientBuilder.create()
    client.addInterceptorFirst(new HttpRequestInterceptor {
      override def process(request: HttpRequest, context: HttpContext): Unit = {
        request.addHeader(HttpHeaders.CONTENT_TYPE, "application/json")
        request.addHeader(
          "Authorization",
          s"Token $varsomeToken"
        )
      }
    })

    client.build()
  }

  def getEntities(locuses: Seq[String]): VarsomeResponse = {
    val locusesBody = locuses.map(l => s""""$l"""").mkString(",")
    val body = s"""{"variants":[$locusesBody]}"""
    val request = new HttpPost(s"$varsomeUrl/lookup/batch/hg38?add-ACMG-annotation=1&add-source-databases=all&add-all-data=0&expand-pubmed-articles=0&add-region-databases=0")
    request.setEntity(new StringEntity(body))
    val response = http.execute(request)
    val status = response.getStatusLine
    val responseBody = EntityUtils.toString(response.getEntity, "UTF-8")
    if (!status.getStatusCode.equals(200)) {
      throw new IllegalStateException(s"Varsome returned an error :code=${status.getStatusCode}, reason=${status.getReasonPhrase}, request body=$body, response body = $responseBody")
    }
    VarsomeResponse(responseBody)
  }

}
