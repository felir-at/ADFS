import akka.actor.{Props, ActorSystem, ActorPath}
import com.typesafe.config.ConfigFactory
import raft.{InMemoryPersistence, RaftActor}

/**
 * Created by kosii on 2014. 10. 18..
 */
object RaftMain extends App {

  val commonConfig = ConfigFactory.load()

  val system = ActorSystem("system", utils.remoteConfig("127.0.0.1", 2551, commonConfig))
  val persistence1 = InMemoryPersistence()
  val persistence2 = InMemoryPersistence()
  val persistence3 = InMemoryPersistence()


  val clusterConfiguration: Map[Int, ActorPath] = Map(
    1 -> ActorPath.fromString("akka://system/user/1"),
    2 -> ActorPath.fromString("akka://system/user/2"),
    3 -> ActorPath.fromString("akka://system/user/3")
  )

  val raft1 = system.actorOf(
    RaftActor.props(
      1, clusterConfiguration, 3, persistence1
    ),
    "1")

}
