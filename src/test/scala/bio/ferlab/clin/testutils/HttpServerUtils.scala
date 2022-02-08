package bio.ferlab.clin.testutils

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}

import java.net.{HttpURLConnection, InetSocketAddress, ServerSocket}
import scala.io.Source

object HttpServerUtils {

  def getPort:Int = {
    val socket = new ServerSocket(0)
    socket.setReuseAddress(true)
    val port = socket.getLocalPort
    port
  }

  def withHttpServer[T](path: String, handler: HttpHandler)(block: String => T): T = {
    val port = getPort
    val address = new InetSocketAddress("localhost",port)
    val server = HttpServer.create(address, 0)
    server.setExecutor(null); // creates a default executor
    server.start()
    server.createContext(path, handler)
    try {
      block(s"http://${address.getHostName}:${port}")
    } finally {
      server.stop(0)
    }
  }

  def resourceHandler(resource: String, contentType: String): HttpHandler = new HttpHandler {
    override def handle(exchange: HttpExchange): Unit = {
      val responseBody = Source.fromResource(resource).mkString.getBytes("UTF-8")
      exchange.getResponseHeaders.add("Content-Type", contentType)
      exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, responseBody.length)
      try {
        exchange.getResponseBody.write(responseBody)
      } catch {
        case e: Throwable => e.printStackTrace()
      }
      exchange.close()
    }
  }
}

