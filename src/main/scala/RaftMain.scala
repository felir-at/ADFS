
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps


import akka.actor.{Inbox, ActorPath, ActorSystem, PoisonPill}
import akka.cluster.Cluster
import akka.pattern.ask
import com.typesafe.config.ConfigFactory


import org.iq80.leveldb._
//import org.iq80.leveldb.impl.Iq80DBFactory._
import org.fusesource.leveldbjni.JniDBFactory._
import java.io._


import raft.cluster._
import raft.persistence.{LevelDBPersistence, InMemoryPersistence}
import raft.statemachine._




/**
 * Created by kosii on 2014. 10. 18..
 */
object RaftMain extends App {

  val commonConfig = ConfigFactory.load("application.conf")

  val system = ActorSystem("system", adfs.utils.remoteConfig("127.0.0.1", 2551, commonConfig))

  val options: Options = new Options()
  options.createIfMissing(true)

  //  val persistence1 = InMemoryPersistence()

  factory.repair(new File("db/persistence1"), options)
  val db1: DB = factory.open(new File("db/persistence1"), options)
  val db2: DB = factory.open(new File("db/persistence2"), options)
  val db3: DB = factory.open(new File("db/persistence3"), options)

  val persistence1 = LevelDBPersistence[Command, Map[String, Int]](db1)

//  val persistence2 = InMemoryPersistence()
//  val persistence3 = InMemoryPersistence()

  val persistence2 = LevelDBPersistence[Command, Map[String, Int]](db2)
  val persistence3 = LevelDBPersistence[Command, Map[String, Int]](db3)

  val clusterConfigurationMap: Map[Int, ActorPath] = Map(
    1 -> ActorPath.fromString("akka://system/user/1"),
    2 -> ActorPath.fromString("akka://system/user/2"),
    3 -> ActorPath.fromString("akka://system/user/3")
  )

  val clusterConfiguration = ClusterConfiguration(clusterConfigurationMap, None)


  implicit val timeout: akka.util.Timeout = akka.util.Timeout(10 seconds)

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


    Thread.sleep(5000)



//    val r = raft1 ? ClientCommand(GetValue("a"))

    implicit val inbox = Inbox.create(system)
    implicit val actorRef = inbox.getRef()
//    println(inbox.getRef())

    raft1 ! ClientCommand(GetValue("a"))
//    raft1 ! ClientCommand(GetValue("a"))
//    raft1.tell
    var r = inbox.receive(5 seconds)

//    val r = Await.result(raft1 ? ClientCommand(GetValue("a")), 1 seconds)

    val leaderId = r match {
      case ReferToLeader(id) => {
        println(s"update leader to ${id}")
        id
      }
      case OK(a) => {
        s"update leader to 1"
        1
      }
      case OK => {
        println("wtf")
        1
      }
    }

    val leaderPath = clusterConfigurationMap(leaderId)

//    system.actorSelection(leaderPath)
    system.actorSelection(leaderPath) ! ClientCommand(SetValue("a", 5))
    val r1 = inbox.receive(3 seconds)
    assert(r1 == OK)
    println(s"Setting 'a' -> 5\nthe result coming back from the cluster: ${r1}")

    //    val r1 = system.actorSelection(leaderPath) ? ClientCommand(SetValue("a", 5))
//    r1.onComplete { t =>
////      assert(t == Success(OK))
//      println(s"Setting 'a' -> 5\nthe result coming back from the cluster: ${t}")
//    }

    Thread.sleep(100)

    system.actorSelection(leaderPath) ! ClientCommand(GetValue("a"))
    val r2 = inbox.receive(3 seconds)
    assert(r2 == OK(Some(5)))
    println(s"Getting 'a'\nthe result coming back from the cluster: ${r2}")
//      r2.onComplete { t =>
////      assert(t == Success(Some(5)))
//      println(s"Getting 'a'\nthe result coming back from the cluster: ${t}")
//    }
    Thread.sleep(100)

    system.actorSelection(leaderPath) ! ClientCommand(SetValue("b", 10))
    val r3 = inbox.receive(3 seconds)
    assert(r3 == OK)
    println(s"Setting 'b' -> 10\nthe result coming back from the cluster: ${r3}")
//    r3.onComplete { t =>
////      assert(t == Success(OK))
//      println(s"Setting 'b' -> 10\nthe result coming back from the cluster: ${t}")
//    }
    Thread.sleep(100)

    system.actorSelection(leaderPath) ! ClientCommand(GetValue("b"))
    val r4 = inbox.receive(3 seconds)
    assert(r4 == OK(Some(10)))
    println(s"Getting 'b'\nthe result coming back from the cluster: ${r4}")
//      r4.onComplete { t =>
////      assert(t == Success(Some(10)))
//      println(s"Getting 'b'\nthe result coming back from the cluster: ${t}")
//    }
    Thread.sleep(100)
//
    system.actorSelection(leaderPath) ! ClientCommand(DeleteValue("a"))
    val r5 = inbox.receive(3 seconds)
    assert(r5 == OK)
    println(s"Deleting 'a'\nthe result coming back from the cluster: ${r5}")
//      r5.onComplete { t =>
////      assert(t == Success(OK))
//      println(s"Deleting 'a'\nthe result coming back from the cluster: ${t}")
//    }
    Thread.sleep(100)
//
//
    system.actorSelection(leaderPath) ! ClientCommand(GetValue("a"))
    val r6 = inbox.receive(3 seconds)
    assert(r6 == OK(None))
    println(s"Getting 'a'\nthe result coming back from the cluster: ${r6}")
//      r6.onComplete { t =>
////      assert(t == Success(None))
//      println(s"Getting 'a'\nthe result coming back from the cluster: ${t}")
//    }
//
    println("sleeping before joing")

    Thread.sleep(1000)

    println("initiate join")
    val persistence4 = InMemoryPersistence()
    val raft4 = system.actorOf(
      RaftActor.props(
        4, clusterConfiguration, 4, persistence4, classOf[KVStore]
      ),
      "4")
    system.actorSelection(leaderPath) ! Join(4, raft4.path)
//
    println("sleeping")
    Thread.sleep(3000)
    println("end sleeping")
    system.shutdown()
    println("killing raft1")
//    raft1 ! PoisonPill

    db1.close()
    system.shutdown()
  }




}
