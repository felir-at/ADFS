package cluster

import java.io.File

import akka.actor.Actor.Receive
import akka.actor.{ActorRef, Props, ActorLogging, Actor}

/**
 * Created by kosii on 2014.10.02..
 */

case class FileSystemActor(path: File, fileRegistry: ActorRef) extends Actor with ActorLogging {

  for {
    file <- path.listFiles()
  } {
    context.actorOf(PhysicalFileActor.props(file))
  }

  override def receive: Receive = {
    case _=>
  }
}
object FileSystemActor {
  def props(path: File, fileRegistry: ActorRef) = Props(classOf[FileSystemActor], path, fileRegistry)
}
