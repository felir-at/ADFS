package cluster

import java.io.File

import akka.actor.{ActorRef, Props, ActorLogging, Actor}
import akka.actor.Actor.Receive
import cluster.FileRegistry.RegisterFile

/**
 * Created by kosii on 2014.09.20..
 */
class PhysicalFileActor(file: File, fileRegistry: ActorRef) extends Actor with ActorLogging {



  override def preStart(): Unit = {
    fileRegistry ! RegisterFile(file.getPath)
  }
  override def receive: Receive = {
    case _ =>
  }

}


object PhysicalFileActor {
  def props(file: File) = Props(classOf[PhysicalFileActor], file)
}