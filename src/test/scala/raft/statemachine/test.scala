package raft.statemachine.test


import org.scalatest.concurrent.ScalaFutures

import scala.language.postfixOps

import akka.util.Timeout
import raft.statemachine
import raft.statemachine._

import collection.mutable.Stack

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.testkit.{TestKit, TestActorRef}
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import org.scalatest._

import scala.util.{Success, Failure, Try}


class ExampleSpec(_system: ActorSystem) extends TestKit(_system) with WordSpecLike with Matchers with BeforeAndAfterAll with ScalaFutures {
//  implicit val system = ActorSystem("MyActorSystem", ConfigFactory.load("test"))
  def this() = this(ActorSystem("MyActorSystem", ConfigFactory.load("test")))


  implicit val timeout = Timeout(3 seconds)

  "A KVStore" should {

    "respond to a Set command" in {
      val actorRef = TestActorRef[KVStore]
      val actor = actorRef.underlyingActor

      val r = actorRef ? SetValue(0, "a", 5)
      whenReady(r) { value =>
        value should be { statemachine.OK }
      }

      val result = r.value.get.get
      result should be { statemachine.OK }
    }


    "respond to multiple Set commands" in {
      val actorRef = TestActorRef[KVStore]
      val actor = actorRef.underlyingActor

      val r1 = actorRef ? SetValue(0, "a", 5)
      whenReady(r1) { value =>
        value should be { statemachine.OK }
      }

      val r2 = actorRef ? SetValue(1, "a", 5)
      whenReady(r2) { value =>
        value should be { statemachine.OK }
      }
    }


    "respond to a Get command with None" in {
      val actorRef = TestActorRef[KVStore]
      val actor = actorRef.underlyingActor


      assert(actor.lastApplied == None)

      val r1 = actorRef ? GetValue("a")
      whenReady(r1) { value =>
        value should be { statemachine.OK(None) }
      }

      assert(actor.lastApplied == None)
    }


    "respond to multiple Get's with None's" in {
      val actorRef = TestActorRef[KVStore]
      val actor = actorRef.underlyingActor

      assert(actor.lastApplied == None)

      val r1 = actorRef ? GetValue("a")
      whenReady(r1) { value =>
        value should be { statemachine.OK(None) }
      }

      assert(actor.lastApplied == None)

      val r2 = actorRef ? GetValue("a")
      whenReady(r2) { value =>
        value should be { statemachine.OK(None) }
      }

      assert(actor.lastApplied == None)
    }


    "after a SetValue respond to a GetValue with the correct value" in {
      val actorRef = TestActorRef[KVStore]
      val actor = actorRef.underlyingActor

      assert(actor.lastApplied == None)

      val r1 = actorRef ? SetValue(0, "a", 5)
      whenReady(r1) { value =>
        value should be { statemachine.OK }
      }

      assert(actor.lastApplied == Some(0))

      val r2 = actorRef ? GetValue("a")
      whenReady(r2) { value =>
        value should be { statemachine.OK(Some(5)) }
      }

      assert(actor.lastApplied == Some(0))
    }

    "respond with RequestOutOfOrder" in {
      val actorRef = TestActorRef[KVStore]
      val actor = actorRef.underlyingActor

      assert(actor.lastApplied == None)
      val r1 = actorRef ? SetValue(1, "a", 5)
      whenReady(r1) { value =>
        value should be { statemachine.RequestOutOfOrder }
      }

      assert(actor.lastApplied == None)
    }

    "correctly update values" in {
      val actorRef = TestActorRef[KVStore]
      val actor = actorRef.underlyingActor

      assert(actor.lastApplied == None)

      val r1 = actorRef ? SetValue(0, "a", 5)

      whenReady(r1) { value =>
        value should be { statemachine.OK }
      }

      assert(actor.lastApplied == Some(0))

      val r2 = actorRef ? GetValue("a")
      whenReady(r2) { value =>
        value should be { statemachine.OK(Some(5)) }
      }
      assert(actor.lastApplied == Some(0))

      val r3 = actorRef ? SetValue(1, "a", 10)
      whenReady(r3) { value =>
        value should be { statemachine.OK }
      }

      assert(actor.lastApplied == Some(1))

      val r4 = actorRef ? GetValue("a")
      whenReady(r4) { value =>
        value should be { statemachine.OK(Some(10)) }
      }

      assert(actor.lastApplied == Some(1))

    }

    "return OK when deleting non-existing key" in {
      val actorRef = TestActorRef[KVStore]
      val actor = actorRef.underlyingActor

      assert(actor.lastApplied == None)

      val r1  = actorRef ? DeleteValue(0, "a")
      whenReady(r1) { value =>
        value should be { statemachine.OK }
      }

      assert(actor.lastApplied == Some(0))
    }

    "return OK when deleting existing key" in {
      val actorRef = TestActorRef[KVStore]
      val actor = actorRef.underlyingActor

      assert(actor.lastApplied == None)

      val r1 = actorRef ? SetValue(0, "a", 5)
      whenReady(r1) { value =>
        value should be { statemachine.OK }
      }

      assert(actor.lastApplied == Some(0))

      val r2 = actorRef ? GetValue("a")
      whenReady(r2) { value =>
        value should be { statemachine.OK(Some(5)) }
      }

      assert(actor.lastApplied == Some(0))

      val r3  = actorRef ? DeleteValue(1, "a")
      whenReady(r3) { value =>
        value should be { statemachine.OK }
      }

      assert(actor.lastApplied == Some(1))


    }

    "not accept out of order indexes" in {

      val actorRef = TestActorRef[KVStore]
      val actor = actorRef.underlyingActor

      assert(actor.lastApplied == None)

      val r1 = actorRef ? DeleteValue(1, "a")
      whenReady(r1) { value =>
        value should be { statemachine.RequestOutOfOrder }
      }

      assert(actor.lastApplied == None)

    }

    "not overwrite value because of a SetValue with old indexes" in {
      val actorRef = TestActorRef[KVStore]
      val actor = actorRef.underlyingActor

      assert(actor.lastApplied == None)

      val r1 = actorRef ? SetValue(0, "a", 5)
      whenReady(r1) { value =>
        value should be { statemachine.OK }
      }

      assert(actor.lastApplied == Some(0))

      val r2 = actorRef ? GetValue("a")
      whenReady(r2) {value =>
        value should be { statemachine.OK(Some(5))}
      }

      assert(actor.lastApplied == Some(0))


      val r3 = actorRef ? SetValue(0, "a", 10)
      whenReady(r3) { value =>
        value should be { statemachine.OK }
      }

      assert(actor.lastApplied == Some(0))

      val r4 = actorRef ? GetValue("a")
      whenReady(r4) {value =>
        value should be { statemachine.OK(Some(5))}
      }

      assert(actor.lastApplied == Some(0))

    }

    "not delete value because of a DeleteValue with old indexes" in {
      val actorRef = TestActorRef[KVStore]
      val actor = actorRef.underlyingActor

      assert(actor.lastApplied == None)

      val r1 = actorRef ? SetValue(0, "a", 5)

      whenReady(r1) { value =>
        value should be { statemachine.OK }
      }

      assert(actor.lastApplied == Some(0))

      val r2 = actorRef ? GetValue("a")
      whenReady(r2) {value =>
        value should be { statemachine.OK(Some(5))}
      }

      assert(actor.lastApplied == Some(0))


      val r3 = actorRef ? DeleteValue(0, "a")
      whenReady(r3) { value =>
        value should be { statemachine.OK }
      }

      assert(actor.lastApplied == Some(0))

      val r4 = actorRef ? GetValue("a")
      whenReady(r4) {value =>
        value should be { statemachine.OK(Some(5))}
      }

      assert(actor.lastApplied == Some(0))

    }



  }

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }
}


