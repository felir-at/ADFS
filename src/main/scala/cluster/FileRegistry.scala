package cluster

import akka.actor.Actor.Receive
import akka.actor.{ActorRef, ActorPath, Actor, ActorLogging}
import akka.cluster.routing.{ClusterRouterGroupSettings, ClusterRouterGroup}
import akka.routing.{ScatterGatherFirstCompletedGroup, BroadcastGroup}
import cluster.FileRegistry._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext

import scala.collection.immutable

/**
 * Created by kosii on 2014.09.27..
 */
class FileRegistry extends Actor with ActorLogging {
  import context._



  override def preStart(): Unit = {
    println(context.self.path.toStringWithoutAddress)

    val quorum = actorOf(
      new ClusterRouterGroup(
        new BroadcastGroup(Nil),
        new ClusterRouterGroupSettings(
          100, List[String]("/user/node/registry"), false, None
        )
      ).props(),
      "quorum")

    //TODO this may be problematic, the quorum members not necessarily are registered yet
    context.system.scheduler.scheduleOnce(1 second, quorum, SyncRequest)(implicitly[ExecutionContext], self)
    quorum ! SyncRequest
  }

  context.self.path

  override def receive: Receive = updatedWith(Map[String, Set[ActorRef]]())

  // TODO: make this to an actual CRDT, it's one of the key points of my thesis
  def merge(mapping: Mapping, mapping1: Mapping): FileRegistry.Mapping = (for {
    key <- mapping.keySet ++ mapping1.keySet
    value1 = mapping.getOrElse(key, Set())
    value2 = mapping.getOrElse(key, Set())
  } yield (key, value1 ++ value2)).toMap

  def updatedWith(mapping: Mapping): Receive = {
    case RegisterFile(path) => {
      log.debug(s"Request received to register $path")

      context.become(
        updatedWith(mapping + (path -> (mapping(path) + sender))))
    }
    case SyncRequest => sender ! SyncResponse(mapping)
    case SyncResponse(_mapping: Mapping) => context.become(updatedWith(merge(mapping, _mapping)))

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