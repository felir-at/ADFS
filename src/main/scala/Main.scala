import java.io.File

import akka.actor.ActorSystem
import akka.cluster.Cluster
import cluster.FileRegistry.RegisterFile
import cluster.{FileStorage, MemberActor}
import com.typesafe.config.ConfigFactory

import utils.remoteConfig

/**
 * Created by kosii on 2014.09.20..
 */
object Main extends App {

  val commonConfig = ConfigFactory.load()
  println(commonConfig)


//  FileStorage("system", "localhost", 2551)
//  val system1 = ActorSystem("system", remoteConfig("127.0.0.1", 2551, commonConfig))
//  system1.actorOf(MemberActor.props(new File("/tmp/system1")), "node")

  val system2 = ActorSystem("system", remoteConfig("127.0.0.1", 2552, commonConfig))
  system2.actorOf(MemberActor.props(new File("/tmp/system2")), "node")

  val system3 = ActorSystem("system", remoteConfig("127.0.0.1", 2553, commonConfig))
  system3.actorOf(MemberActor.props(new File("/tmp/system3")), "node")

  val system4 = ActorSystem("system", remoteConfig("127.0.0.1", 2554, commonConfig))
  system4.actorOf(MemberActor.props(new File("/tmp/system4")), "node")

  Cluster(system4) registerOnMemberUp {
    println("We are up!!!!!!!!")
    system4.actorSelection("/user/node/registry/quorum") ! "sdfsd"
    system4.actorSelection("/user/node/registry/quorum") ! RegisterFile("/sfsdf/dsf")


    val system5 = ActorSystem("system", remoteConfig("127.0.0.1", 2555, commonConfig))
    system5.actorOf(MemberActor.props(new File("/tmp/system5")), "node")

    Cluster(system5) registerOnMemberUp {
      Thread.sleep(500)
      system4.actorSelection("/user/node/registry/quorum") ! RegisterFile("Y0000000")
    }

  }

}
