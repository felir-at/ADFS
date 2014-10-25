package raft.statemachine.test

import scala.language.postfixOps

import akka.util.Timeout
import raft.statemachine
import raft.statemachine.{GetValue, OK, SetValue, KVStore}

import collection.mutable.Stack

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.testkit.{TestKit, TestActorRef}
import com.typesafe.config.ConfigFactory
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import org.scalatest._

import scala.util.{Success, Failure, Try}


class ExampleSpec(_system: ActorSystem) extends TestKit(_system) with WordSpecLike with Matchers with BeforeAndAfterAll {
//  implicit val system = ActorSystem("MyActorSystem", ConfigFactory.load("test"))
  def this() = this(ActorSystem("MyActorSystem", ConfigFactory.load("test")))


  implicit val timeout = Timeout(3 seconds)

  "A KVStore" should {

    "respond to a Set command" in {
      val actorRef = TestActorRef[KVStore]
      val actor = actorRef.underlyingActor

      val r = actorRef ? SetValue(1, "a", 5)
      r onComplete {
        case Success(v) => v should be { statemachine.OK }
        case Failure(th) => assert(false)
      }

      val result = r.value.get.get
      result should be { statemachine.OK }
    }


    "respond to multiple Set commands" in {
      val actorRef = TestActorRef[KVStore]
      val actor = actorRef.underlyingActor

      val r1 = actorRef ? SetValue(1, "a", 5)
      r1 onComplete {
        case Success(v) => v should be { statemachine.OK }
        case Failure(th) => assert(false)
      }

      val r2 = actorRef ? SetValue(1, "a", 5)
      r2 onComplete {
        case Success(v) => v should be { statemachine.OK }
        case Failure(th) => assert(false)
      }
    }


    "respond to a Get command with None" in {
      val actorRef = TestActorRef[KVStore]
      val actor = actorRef.underlyingActor

      val r1 = actorRef ? GetValue(1, "a")
      r1 onComplete {
        case Success(v) => v should be { statemachine.OK(None) }
        case Failure(th) => assert(false)
      }
    }


    "respond to multiple Get's with None's" in {
      val actorRef = TestActorRef[KVStore]
      val actor = actorRef.underlyingActor

      val r1 = actorRef ? GetValue(1, "a")
      r1 onComplete {
        case Success(v) => v should be { statemachine.OK(None) }
        case Failure(th) => assert(false)
      }

      val r2 = actorRef ? GetValue(1, "a")
      r2 onComplete {
        case Success(v) => v should be { statemachine.OK(None) }
        case Failure(th) => assert(false)
      }
    }


    "after a SetValue respond to a GetValue with the correct value" in {
      val actorRef = TestActorRef[KVStore]
      val actor = actorRef.underlyingActor

      val r1 = actorRef ? SetValue(1, "a", 5)
      r1 onComplete {
        case Success(v) => v should be { statemachine.OK }
        case Failure(th) => assert(false)
      }

      val r2 = actorRef ? GetValue(1, "a")
      r2 onComplete {
        case Success(v) => v should be { statemachine.OK(Some(5)) }
        case Failure(th) => assert(false)
      }
    }

  }

  "A Stack" should {
    "pop values in last-in-first-out order" in {
      val stack = new Stack[Int]
      stack.push(1)
      stack.push(2)
      stack.pop() should be(2)
      stack.pop() should be(1)
    }
  }

  it should {
    "throw NoSuchElementException if an empty stack is popped" in {
      val emptyStack = new Stack[Int]
      a [NoSuchElementException] should be thrownBy {
        emptyStack.pop()
      }
    }
  }

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }
}


