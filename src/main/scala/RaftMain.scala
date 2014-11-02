import akka.actor.{ActorPath, ActorSystem, PoisonPill}
import akka.cluster.Cluster
import akka.pattern.ask
import com.typesafe.config.ConfigFactory
import raft.cluster.{ClientCommand, ClusterConfiguration, RaftActor}
import raft.persistence.InMemoryPersistence
import raft.statemachine.{GetValue, Command, KVStore, SetValue}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps

/**
 * Created by kosii on 2014. 10. 18..
 */
object RaftMain extends App {

  val commonConfig = ConfigFactory.load()

  val system = ActorSystem("system", adfs.utils.remoteConfig("127.0.0.1", 2551, commonConfig))

  val persistence1 = InMemoryPersistence()
  val persistence2 = InMemoryPersistence()
  val persistence3 = InMemoryPersistence()


  val clusterConfigurationMap: Map[Int, ActorPath] = Map(
    1 -> ActorPath.fromString("akka://system/user/1"),
    2 -> ActorPath.fromString("akka://system/user/2"),
    3 -> ActorPath.fromString("akka://system/user/3")
  )

  val clusterConfiguration = ClusterConfiguration(clusterConfigurationMap, Map(), None)


  Cluster(system).registerOnMemberUp {

    val raft1 = system.actorOf(
      RaftActor.props[Command, Map[String, Int], KVStore](
        1, clusterConfiguration, 3, persistence1, classOf[KVStore]
      ),
      "1")

    val raft2 = system.actorOf(
      RaftActor.props(
        2, clusterConfiguration, 3, persistence2, classOf[KVStore]
      ),
      "2")

    val raft3 = system.actorOf(
      RaftActor.props(
        3, clusterConfiguration, 3, persistence3, classOf[KVStore]
      ),
      "3")


    Thread.sleep(3000)

    implicit val timeout: akka.util.Timeout = akka.util.Timeout(1 seconds)
    val r1 = raft1 ? ClientCommand(SetValue("a", 5))
    r1.onComplete { t => println(s"the result coming back from the cluster: ${t}") }

    val r2 = raft2 ? ClientCommand(SetValue("a", 5))
    r2.onComplete({ t => println(s"the result coming back from the cluster: ${t}") })

    val r3 = raft3 ? ClientCommand(SetValue("a", 5))
    r3.onComplete({ t => println(s"the result coming back from the cluster: ${t}") })


    val r21 = raft1 ? ClientCommand(GetValue("a"))
    r21.onComplete { t => println(s"the result coming back from the cluster: ${t}") }
    val r22 = raft2 ? ClientCommand(GetValue("a"))
    r22.onComplete { t => println(s"the result coming back from the cluster: ${t}") }
    val r23 = raft3 ? ClientCommand(GetValue("a"))
    r23.onComplete { t => println(s"the result coming back from the cluster: ${t}") }


    println("sleeping")
    Thread.sleep(30000)
    println("end sleeping")

    println("killing raft1")
    raft1 ! PoisonPill

  }




}
