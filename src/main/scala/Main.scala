import java.io.File

import akka.actor.ActorSystem
import akka.cluster.Cluster
import cluster.MemberActor
import com.typesafe.config.ConfigFactory

import utils.remoteConfig

/**
 * Created by kosii on 2014.09.20..
 */
object Main extends App {

  val commonConfig = ConfigFactory.load()

  val system1 = ActorSystem("system", remoteConfig("127.0.0.1", 2551, commonConfig))
  system1.actorOf(MemberActor.props(new File("/tmp/system1")))

  val system2 = ActorSystem("system", remoteConfig("127.0.0.1", 2552, commonConfig))
  system2.actorOf(MemberActor.props(new File("/tmp/system2")))

  val system3 = ActorSystem("system", remoteConfig("127.0.0.1", 2553, commonConfig))
  system3.actorOf(MemberActor.props(new File("/tmp/system3")))

  val system4 = ActorSystem("system", remoteConfig("127.0.0.1", 2554, commonConfig))
  system4.actorOf(MemberActor.props(new File("/tmp/system4")))

}
