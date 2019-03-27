import java.net.{InetAddress, Socket}
import java.io.Writer
import java.io.PrintWriter

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.apache.spark.rdd.RDD

import scala.collection.mutable.Queue

object DAGVisualizer {
  val log = LoggerFactory.getLogger(classOf[DAGVisualizer]);

  private lazy val _inst = new DAGVisualizer("127.0.1.1", 8001)
  def get = _inst
}

class DAGVisualizer(val host: String,
                    val port: Int) {
  var sock : Socket = _
  var writer : Writer = _

  def logWarn(str: String) = DAGVisualizer.log.warn(str)
  def logInfo(str: String) = DAGVisualizer.log.info(str)
  def logDebug(str: String) = DAGVisualizer.log.debug(str)

  private def connect(host: String, port: Int): Boolean = {
    try {
      sock = new Socket(InetAddress.getByName(host),
                          port)
      writer = new PrintWriter(sock.getOutputStream())
      true
    } catch {
      case e: Throwable =>
        logWarn("Failed to connect to visualizer.");
        sock = null;
        false
    }
  }

  if (connect(host, port)) {
    logInfo("Connected to visualizer.")
  }

  def clear(): Unit = {
    if(sock == null || sock.isClosed) {
      logDebug("Not connected.");
    } else {
      logDebug("clear")
      writer.write("clear\n");
      writer.flush()
    }
  }

  def submitRDD[T](rdd: RDD[T]): Unit = {
    if(sock == null || sock.isClosed) {
      logDebug("Not connected.");
    } else {
      val sb = new StringBuilder()

      sb ++= rdd.id.toString
      sb += ' '
      sb ++= rdd.dependencies.length.toString

      for(dep <- rdd.dependencies) {
        sb ++= ' ' + dep.rdd.id.toString
      }

      sb += '\n'

      writer.write(sb.toString)
      writer.flush()
    }
  }

  def submitAll[T](rdd: RDD[T]): Unit = {
    if(sock == null || sock.isClosed) {
      logDebug("Not connected.");
    } else {
      val q = new Queue[RDD[Any]]()
      q.enqueue(rdd.asInstanceOf[RDD[Any]])

      while(!q.isEmpty) {
        val rdd = q.dequeue
        logDebug("submitting: " + rdd.toString)
        submitRDD(rdd)

        for(dep <- rdd.dependencies) {
          q.enqueue(dep.rdd.asInstanceOf[RDD[Any]])
        }
      }
    }
  }
}
