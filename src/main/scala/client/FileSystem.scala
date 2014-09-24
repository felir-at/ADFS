package client

import akka.actor.ActorSystem
import akka.util.ByteString
import operations.{DataRead, WriteDone}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import utils.remoteConfig
/**
 *
 */
class FileSystem(cluster: Seq[String]) {


  val system = ActorSystem("clientSystem", remoteConfig("127.0.0.1", 2551))

  def open(fileName: String, mode: String) = ???
  def create(fileName: String): File = ???
  def close() = ???

}

//TODO : refactor this: we need to pass a router to the file instead of a filename, and we need to make the constructor private
//TODO:  we need to create a factory on the object file

case class File(fileName: String) {

  def write(offset: Int, data: ByteString): Future[WriteDone] = Future {
    ???
  }
  def read(offset: Int = 0): Future[DataRead] = Future {
    ???
  }

}