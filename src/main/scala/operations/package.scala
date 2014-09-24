import java.io.ByteArrayInputStream

import akka.util.ByteString

/**
 * Created by kosii on 2014.09.20..
 */
package object operations {

  /** The common trait for all operations allowed to be sent to an actor
   *
   */
  trait AbstractOperation

  trait RequestOperation extends AbstractOperation

  /** Write
   *
   */
  case class Write(offset: Int, data: ByteString) extends RequestOperation
  case class Read() extends RequestOperation
  case class Delete(fileName: String) extends RequestOperation


  trait ResponseOperation extends AbstractOperation

  case class WriteDone() extends ResponseOperation
  case class DataRead(data: ByteString) extends ResponseOperation
  case class Deleted() extends ResponseOperation
}
