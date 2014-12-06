package raft.statemachine.test


import akka.actor.ActorSystem
import akka.pattern.ask
import akka.testkit.{TestActorRef, TestKit}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import raft.statemachine
import raft.statemachine._

import scala.concurrent.duration._
import scala.language.postfixOps


class KVStoreSpec(_system: ActorSystem) extends TestKit(_system) with WordSpecLike with Matchers with BeforeAndAfterAll with ScalaFutures {
//  implicit val system = ActorSystem("MyActorSystem", ConfigFactory.load("test"))
  def this() = this(ActorSystem("MyActorSystem", ConfigFactory.load("test")))


  implicit val timeout = Timeout(3 seconds)

  "A KVStore" should {

    "respond to a Set command" in {
      val actorRef = TestActorRef[KVStore]
      val actor = actorRef.underlyingActor

      val r = actorRef ? (1, SetValue("a", 5))
      whenReady(r) { value =>
        value should be { statemachine.OK }
      }

      actor.stop()

    }


    "respond to multiple Set commands" in {
      val actorRef = TestActorRef[KVStore]
      val actor = actorRef.underlyingActor

      val r1 = actorRef ? (1, SetValue("a", 5))
      whenReady(r1) { value =>
        value should be { statemachine.OK }
      }

      val r2 = actorRef ? (2, SetValue("a", 5))
      whenReady(r2) { value =>
        value should be { statemachine.OK }
      }

      actor.stop()
    }


    "respond to a Get command with None" in {
      val actorRef = TestActorRef[KVStore]
      val actor = actorRef.underlyingActor


      assert(actor.stateMachineDurability.getLastApplied == -1)

      val r1 = actorRef ? (1, GetValue("a"))
      whenReady(r1) { value =>
        value should be { statemachine.OK(None) }
      }

      assert(actor.stateMachineDurability.getLastApplied == 1)

      actor.stop()
    }


    "respond to multiple Get's with None's" in {
      val actorRef = TestActorRef[KVStore]
      val actor = actorRef.underlyingActor

      assert(actor.stateMachineDurability.getLastApplied == None)

      val r1 = actorRef ? (1, GetValue("a"))
      whenReady(r1) { value =>
        value should be { statemachine.OK(None) }
      }

      assert(actor.stateMachineDurability.getLastApplied == Some(1))

      val r2 = actorRef ? (2, GetValue("a"))
      whenReady(r2) { value =>
        value should be { statemachine.OK(None) }
      }

      assert(actor.stateMachineDurability.getLastApplied == Some(2))

      actor.stop()
    }


    "after a SetValue respond to a GetValue with the correct value" in {
      val actorRef = TestActorRef[KVStore]
      val actor = actorRef.underlyingActor

      assert(actor.stateMachineDurability.getLastApplied == None)

      val r1 = actorRef ? (1, SetValue("a", 5))
      whenReady(r1) { value =>
        value should be { statemachine.OK }
      }

      assert(actor.stateMachineDurability.getLastApplied == Some(1))

      val r2 = actorRef ? (2, GetValue("a"))
      whenReady(r2) { value =>
        value should be { statemachine.OK(Some(5)) }
      }

      assert(actor.stateMachineDurability.getLastApplied == Some(2))
      actor.stop()
    }

    "respond with None" in {
      val actorRef = TestActorRef[KVStore]
      val actor = actorRef.underlyingActor

      assert(actor.stateMachineDurability.getLastApplied == None)
      val r1 = actorRef ? (1, SetValue("a", 5))
      whenReady(r1) { value =>
        value should be { statemachine.OK }
      }

      assert(actor.stateMachineDurability.getLastApplied == Some(1))
      actor.stop()
    }

    "correctly update values" in {
      val actorRef = TestActorRef[KVStore]
      val actor = actorRef.underlyingActor

      assert(actor.stateMachineDurability.getLastApplied == None)

      val r1 = actorRef ? (1, SetValue("a", 5))

      whenReady(r1) { value =>
        value should be { statemachine.OK }
      }

      assert(actor.stateMachineDurability.getLastApplied == Some(1))

      val r2 = actorRef ? (2, GetValue("a"))
      whenReady(r2) { value =>
        value should be { statemachine.OK(Some(5)) }
      }
      assert(actor.stateMachineDurability.getLastApplied == Some(2))

      val r3 = actorRef ? (3, SetValue("a", 10))
      whenReady(r3) { value =>
        value should be { statemachine.OK }
      }

      assert(actor.stateMachineDurability.getLastApplied == Some(3))

      val r4 = actorRef ? (4, GetValue("a"))
      whenReady(r4) { value =>
        value should be { statemachine.OK(Some(10)) }
      }

      assert(actor.stateMachineDurability.getLastApplied == Some(4))

      actor.stop()
    }

    "return OK when deleting non-existing key" in {
      val actorRef = TestActorRef[KVStore]
      val actor = actorRef.underlyingActor

      assert(actor.stateMachineDurability.getLastApplied == None)

      val r1  = actorRef ? (1, DeleteValue("a"))
      whenReady(r1) { value =>
        value should be { statemachine.OK }
      }

      assert(actor.stateMachineDurability.getLastApplied == Some(1))
      actor.stop()
    }

    "return OK when deleting existing key" in {
      val actorRef = TestActorRef[KVStore]
      val actor = actorRef.underlyingActor

      assert(actor.stateMachineDurability.getLastApplied == None)

      val r1 = actorRef ? (0, SetValue("a", 5))
      whenReady(r1) { value =>
        value should be { statemachine.OK }
      }

      assert(actor.stateMachineDurability.getLastApplied == Some(0))

      val r2 = actorRef ? (1, GetValue("a"))
      whenReady(r2) { value =>
        value should be { statemachine.OK(Some(5)) }
      }

      assert(actor.stateMachineDurability.getLastApplied == Some(1))

      val r3  = actorRef ? (2, DeleteValue("a"))
      whenReady(r3) { value =>
        value should be { statemachine.OK }
      }

      assert(actor.stateMachineDurability.getLastApplied == Some(2))

      actor.stop()
    }

    "not accept out of order indexes" in {

      val actorRef = TestActorRef[KVStore]
      val actor = actorRef.underlyingActor

      assert(actor.stateMachineDurability.getLastApplied == None)

      val r1 = actorRef ? (1, DeleteValue("a"))
      whenReady(r1) { value =>
        value should be { statemachine.OK }
      }

      assert(actor.stateMachineDurability.getLastApplied == Some(1))

      val r2 =actorRef ? (0, GetValue("a"))
      whenReady(r2) { value =>
        value should be { statemachine.RequestOutOfOrder }
      }

      assert(actor.stateMachineDurability.getLastApplied == Some(1))

      actor.stop()
    }

    "not overwrite value because of a SetValue with old indexes" in {
      val actorRef = TestActorRef[KVStore]
      val actor = actorRef.underlyingActor

      assert(actor.stateMachineDurability.getLastApplied == None)

      val r1 = actorRef ? (0, SetValue("a", 5))
      whenReady(r1) { value =>
        value should be { statemachine.OK }
      }

      assert(actor.stateMachineDurability.getLastApplied == Some(0))

      val r2 = actorRef ? (1, GetValue("a"))
      whenReady(r2) {value =>
        value should be { statemachine.OK(Some(5))}
      }

      assert(actor.stateMachineDurability.getLastApplied == Some(1))


      val r3 = actorRef ? (1, SetValue("a", 10))
      whenReady(r3) { value =>
        value should be { statemachine.OK }
      }

      assert(actor.stateMachineDurability.getLastApplied == Some(1))

      val r4 = actorRef ? (2, GetValue("a"))
      whenReady(r4) {value =>
        value should be { statemachine.OK(Some(5))}
      }

      assert(actor.stateMachineDurability.getLastApplied == Some(2))

      actor.stop()
    }

    "not delete value because of a DeleteValue with old indexes" in {
      val actorRef = TestActorRef[KVStore]
      val actor = actorRef.underlyingActor

      assert(actor.stateMachineDurability.getLastApplied == None)

      val r1 = actorRef ? (0, SetValue("a", 5))

      whenReady(r1) { value =>
        value should be { statemachine.OK }
      }

      assert(actor.stateMachineDurability.getLastApplied == Some(0))

      val r2 = actorRef ? (1 ,GetValue("a"))
      whenReady(r2) {value =>
        value should be { statemachine.OK(Some(5))}
      }

      assert(actor.stateMachineDurability.getLastApplied == Some(1))


      val r3 = actorRef ? (0, DeleteValue("a"))
      whenReady(r3) { value =>
        value should be { statemachine.OK }
      }

      assert(actor.stateMachineDurability.getLastApplied == Some(1))

      val r4 = actorRef ? (2, GetValue("a"))
      whenReady(r4) {value =>
        value should be { statemachine.OK(Some(5))}
      }

      assert(actor.stateMachineDurability.getLastApplied == Some(2))

      actor.stop()
    }



  }

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }
}


