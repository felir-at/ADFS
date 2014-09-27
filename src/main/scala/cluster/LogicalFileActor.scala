package cluster

import akka.actor.Actor.Receive
import akka.actor.{ActorPath, ActorLogging, Actor}

/**
 * Created by kosii on 2014.09.27..
 */

class LogicalFileActor(path: ActorPath) extends Actor with ActorLogging {
  override def preStart(): Unit = {

  }
  override def receive: Receive = {
    case _ =>
  }
}


object LogicalFileActor {

}
