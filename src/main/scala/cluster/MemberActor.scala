package cluster

import java.io.File

import akka.actor.{Props, ActorLogging, Actor}
import akka.cluster.Cluster

/**
 * Created by kosii on 2014.09.20..
 */


class MemberActor(dataDir: File) extends Actor with ActorLogging {
  val cluster = Cluster(context.system)

  for {
    file <- dataDir.listFiles()
  } context.actorOf(PhysicalFileActor.props(file))


  override def receive: Receive = {
    case a => println(a)
  }
}

object MemberActor {
  def props(dataDir: File) = Props(classOf[MemberActor], dataDir)
}