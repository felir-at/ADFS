package cluster

import java.io.File

import akka.actor.{Props, ActorLogging, Actor}
import akka.actor.Actor.Receive

/**
 * Created by kosii on 2014.09.20..
 */
class PhysicalFileActor(file: File) extends Actor with ActorLogging {
  override def receive: Receive = {
    case _ =>
  }

}


object PhysicalFileActor {
  def props(file: File) = Props(classOf[PhysicalFileActor], file)
}