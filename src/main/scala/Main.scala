import cluster.FileStorage
import com.typesafe.config.ConfigFactory

/**
 * Created by kosii on 2014.09.20..
 */
object Main extends App {

  val commonConfig = ConfigFactory.load()
  println(commonConfig)


  FileStorage("system", "127.0.0.1", 2551)
  FileStorage("system", "127.0.0.1", 2552)
  FileStorage("system", "127.0.0.1", 2553)
  FileStorage("system", "127.0.0.1", 2554)
  FileStorage("system", "127.0.0.1", 2555)
//  val system1 = ActorSystem("system", rfemoteConfig("127.0.0.1", 2551, commonConfig))
//  system1.actorOf(MemberActor.props(new File("/tmp/system1")), "node")

//  val system2 = ActorSystem("system", remoteConfig("127.0.0.1", 2552, commonConfig))
//  system2.actorOf(MemberActor.props(new File("/tmp/system2")), "node")

//  val system3 = ActorSystem("system", remoteConfig("127.0.0.1", 2553, commonConfig))
//  system3.actorOf(MemberActor.props(new File("/tmp/system3")), "node")
//
//  val system4 = ActorSystem("system", remoteConfig("127.0.0.1", 2554, commonConfig))
//  system4.actorOf(MemberActor.props(new File("/tmp/system4")), "node")
//
//  Cluster(system4) registerOnMemberUp {
//    println("We are up!!!!!!!!")
//    system4.actorSelection("/user/node/registry/quorum") ! "sdfsd"
//    system4.actorSelection("/user/node/registry/quorum") ! RegisterFile("/sfsdf/dsf")
//
//
//    val system5 = ActorSystem("system", remoteConfig("127.0.0.1", 2555, commonConfig))
//    system5.actorOf(MemberActor.props(new File("/tmp/system5")), "node")
//
//    Cluster(system5) registerOnMemberUp {
//      Thread.sleep(500)
//      system4.actorSelection("/user/node/registry/quorum") ! RegisterFile("Y0000000")
//    }
//
//  }

}
