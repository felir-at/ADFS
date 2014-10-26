import akka.actor.{PoisonPill, Props, ActorSystem, ActorPath}
import akka.cluster.Cluster
import cluster.{PropFactory, ClusterConfiguration, RaftActor}
import com.typesafe.config.ConfigFactory
import raft.persistence.InMemoryPersistence
import raft.statemachine.KVStore

/**
 * Created by kosii on 2014. 10. 18..
 */
object RaftMain extends App {

  val commonConfig = ConfigFactory.load()

  val system = ActorSystem("system", utils.remoteConfig("127.0.0.1", 2551, commonConfig))

  val persistence1 = InMemoryPersistence()
  val persistence2 = InMemoryPersistence()
  val persistence3 = InMemoryPersistence()


  val clusterConfigurationMap: Map[Int, ActorPath] = Map(
    1 -> ActorPath.fromString("akka://system/user/1"),
    2 -> ActorPath.fromString("akka://system/user/2"),
    3 -> ActorPath.fromString("akka://system/user/3")
  )

  val clusterConfiguration = ClusterConfiguration(clusterConfigurationMap, Map(), None)

  def KVFactory: PropFactory[KVStore] = Props(_)

  Cluster(system).registerOnMemberUp {

    val raft1 = system.actorOf(
      RaftActor.props[(String, Int), Map[String, Int], KVStore](
        1, clusterConfiguration, 3, persistence1, KVFactory
      ),
      "1")

    val raft2 = system.actorOf(
      RaftActor.props(
        2, clusterConfiguration, 3, persistence2, KVFactory
      ),
      "2")

    val raft3 = system.actorOf(
      RaftActor.props(
        3, clusterConfiguration, 3, persistence3, KVFactory
      ),
      "3")
    println("sleeping")
    Thread.sleep(3000)
    println("end sleeping")

    println("killing raft1")
    raft1 ! PoisonPill

  }




}
