package raft.cluster.test

import akka.actor.{ActorSystem, Address, RootActorPath}
import akka.testkit.{TestFSMRef, TestKit}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import raft.cluster.{Follower, ClusterConfiguration, RaftActor}
import raft.persistence.InMemoryPersistence
import raft.statemachine.KVStore

import scala.concurrent.duration._
import scala.language.postfixOps

/**
 * Created by kosii on 2014. 11. 01..
 */
class RaftActorObjectSpecs extends WordSpecLike with Matchers with BeforeAndAfterAll with ScalaFutures {


  "determineCommitIndex" should {
    "throw AssertionError when ClusterConfiguration is empty" in {
      an [AssertionError] should be thrownBy RaftActor.determineCommitIndex(ClusterConfiguration(Map(), Map(), None), Map())
      an [AssertionError] should be thrownBy RaftActor.determineCommitIndex(ClusterConfiguration(Map(), Map(), None), Map(1 -> None, 2 -> Some(2), 4->Some(1)))
    }

    "return correctly when there is no changement in the cluster" in {
      val fakeActorPath = RootActorPath(Address("akka.tcp", "system"))
      val currentClusterMap = Map(1 -> fakeActorPath / "a", 2 -> fakeActorPath / "b", 3 -> fakeActorPath / "c")
      val clusterConfiguration = ClusterConfiguration(currentClusterMap, Map(), None)

      RaftActor.determineCommitIndex(clusterConfiguration, Map(1 -> Some(2), 2 -> Some(3), 3 -> Some(2))) should be { Some(2) }
      RaftActor.determineCommitIndex(clusterConfiguration, Map(1 -> Some(2), 2 -> Some(3), 3 -> None)) should be { Some(2) }
      RaftActor.determineCommitIndex(clusterConfiguration, Map(1 -> Some(2), 2 -> None, 3 -> None)) should be { None }
      RaftActor.determineCommitIndex(clusterConfiguration, Map(1 -> Some(2), 2 -> Some(1), 3 -> None)) should be { Some(1) }

    }

    "return correctly when member is joining" in {
      val fakeActorPath = RootActorPath(Address("akka.tcp", "system"))
      val currentClusterMap = Map(1 -> fakeActorPath / "a", 2 -> fakeActorPath / "b", 3 -> fakeActorPath / "c")
      val newClusterMap = Map(1 -> fakeActorPath / "a", 2 -> fakeActorPath / "b", 3 -> fakeActorPath / "c", 4 -> fakeActorPath / "d")
      val clusterConfiguration = ClusterConfiguration(currentClusterMap, newClusterMap, None)

      RaftActor.determineCommitIndex(clusterConfiguration, Map(1 -> Some(2), 2 -> Some(3), 3 -> Some(2))) should be { Some(2) }
      RaftActor.determineCommitIndex(clusterConfiguration, Map(1 -> Some(2), 2 -> Some(3), 3 -> None)) should be { None }
      RaftActor.determineCommitIndex(clusterConfiguration, Map(1 -> Some(2), 2 -> None, 3 -> None)) should be { None }
      RaftActor.determineCommitIndex(clusterConfiguration, Map(1 -> Some(2), 2 -> Some(1), 3 -> None)) should be { None }
      RaftActor.determineCommitIndex(clusterConfiguration, Map(1 -> Some(2), 3 -> None)) should be { None }
    }

    "be symmetric to leave and join" in {
      val fakeActorPath = RootActorPath(Address("akka.tcp", "system"))
      val currentClusterMap = Map(1 -> fakeActorPath / "a", 2 -> fakeActorPath / "b", 3 -> fakeActorPath / "c", 4 -> fakeActorPath / "d")
      val newClusterMap = Map(1 -> fakeActorPath / "a", 2 -> fakeActorPath / "b", 3 -> fakeActorPath / "c")
      val clusterConfiguration = ClusterConfiguration(currentClusterMap, newClusterMap, None)

      RaftActor.determineCommitIndex(clusterConfiguration, Map(1 -> Some(2), 2 -> Some(3), 3 -> Some(2))) should be { Some(2) }
      RaftActor.determineCommitIndex(clusterConfiguration, Map(1 -> Some(2), 2 -> Some(3), 3 -> None)) should be { None }
      RaftActor.determineCommitIndex(clusterConfiguration, Map(1 -> Some(2), 2 -> None, 3 -> None)) should be { None }
      RaftActor.determineCommitIndex(clusterConfiguration, Map(1 -> Some(2), 2 -> Some(1), 3 -> None)) should be { None }
      RaftActor.determineCommitIndex(clusterConfiguration, Map(1 -> Some(2), 3 -> None)) should be { None }

    }

    "return correctly when member is leaving" in {
      val fakeActorPath = RootActorPath(Address("akka.tcp", "system"))
      val currentClusterMap = Map(1 -> fakeActorPath / "a", 2 -> fakeActorPath / "b", 3 -> fakeActorPath / "c")
      val newClusterMap = Map(1 -> fakeActorPath / "a", 2 -> fakeActorPath / "b")
      val clusterConfiguration = ClusterConfiguration(currentClusterMap, newClusterMap, None)

      RaftActor.determineCommitIndex(clusterConfiguration, Map(1 -> Some(2), 2 -> Some(3), 3 -> Some(2))) should be { Some(2) }
      RaftActor.determineCommitIndex(clusterConfiguration, Map(1 -> Some(2), 2 -> Some(3), 3 -> None)) should be { Some(2) }
      RaftActor.determineCommitIndex(clusterConfiguration, Map(1 -> Some(2), 2 -> None, 3 -> None)) should be { None }
      RaftActor.determineCommitIndex(clusterConfiguration, Map(1 -> Some(2), 2 -> Some(1), 3 -> None)) should be { Some(1) }
      RaftActor.determineCommitIndex(clusterConfiguration, Map(1 -> Some(2), 3 -> None)) should be { None }
    }


  }

}

class RaftActorSpecs(_system: ActorSystem) extends TestKit(_system) with WordSpecLike with Matchers with BeforeAndAfterAll with ScalaFutures {
  //  implicit val system = ActorSystem("MyActorSystem", ConfigFactory.load("test"))
  def this() = this(ActorSystem("MyActorSystem", ConfigFactory.load("test")))


  implicit val timeout = Timeout(3 seconds)

  "A RaftActor" should {

    "start in FollowerState" in {
      val persistence = InMemoryPersistence()
      val fakeActorPath = RootActorPath(Address("akka.tcp", "system"))
      val clusterConfiguration = ClusterConfiguration(Map(1 -> fakeActorPath/"1"), Map(), None)
//      val props =
      val actorRef = TestFSMRef(new RaftActor(1, clusterConfiguration, 1, persistence, classOf[KVStore]))
      val actor = actorRef.underlyingActor

      actor.stateName should be { Follower }



    }
  }


  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }
}
