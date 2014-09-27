package cluster

import akka.actor.Actor.Receive
import akka.actor.{ActorRef, ActorPath, Actor, ActorLogging}
import akka.cluster.routing.{ClusterRouterGroupSettings, ClusterRouterGroup}
import akka.routing.BroadcastGroup
import cluster.FileRegistry._

import scala.collection.immutable

/**
 * Created by kosii on 2014.09.27..
 */
class FileRegistry extends Actor with ActorLogging {

  override def preStart(): Unit = {
    println(context.self.path.toStringWithoutAddress)
    context.actorOf(new ClusterRouterGroup(new BroadcastGroup(Nil), new ClusterRouterGroupSettings(
      100, List[String]("/user/node/registry"), true, None
    )).props(), "quorum")
  }

  context.self.path

  override def receive: Receive = receiveFactory(Map[String, Set[ActorRef]]())

  def receiveFactory(mapping: Mapping): Receive = {
    case RegisterFile(path) => {
      log.debug(s"Request received to register $path")

      context.become(receiveFactory(mapping + (path->(mapping(path) + sender))))
    }
    case SyncRequest => sender ! SyncResponse(mapping)
    case _ => println("WTF??????")
  }
}

object FileRegistry {
  type Mapping = Map[String, Set[ActorRef]]


  trait Operation
//  case class RegisterFile(path: ActorPath) extends Operations
  case class RegisterFile(path: String) extends Operation
  case object SyncRequest extends Operation

  case class SyncResponse(mapping: Mapping)


}