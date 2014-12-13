package raft.statemachine

import raft.statemachine
import raft.statemachine.RaftStateMachineAdaptorSpecs.MockRaftStateMachine

import scala.concurrent.duration._

import akka.actor.{ActorPath, FSM, ActorSystem}
import akka.pattern.ask
import akka.testkit.{TestActorRef, TestKit}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

/**
 * Created by kosii on 14. 12. 08..
 */

object RaftStateMachineAdaptorSpecs {

  class MockRaftStateMachine extends FSM[Any, Any] with RaftStateMachineAdaptor[Any, Any] with StateMachine[Any, Any] {
    startWith(None, None)

    when(None) {
      case Event(_, None) => stay
    }

    initialize()
    override type T = InMemoryStateMachine
    override val stateMachineDurability: T = InMemoryStateMachine()
  }

//  class MockRaftStateMachine extends MockStateMachine with RaftStateMachineAdaptor[Any, Any] {
//
//  }
}

class RaftStateMachineAdaptorSpecs (_system: ActorSystem) extends TestKit(_system) with WordSpecLike with Matchers with BeforeAndAfterAll with ScalaFutures {
  //  implicit val system = ActorSystem("MyActorSystem", ConfigFactory.load("test"))
  def this() = this(ActorSystem("MyActorSystem", ConfigFactory.load("test")))

  implicit val timeout = Timeout(3 seconds)
  val testActorPath = ActorPath.fromString("akka.tcp://system/user/a")

  "RaftAdaptorShould" should {
    "respond with AlreadyApplied if index is too low" in {
      val actorRef = TestActorRef[MockRaftStateMachine]
      val actor = actorRef.underlyingActor

      val r = actorRef ? WrappedClientCommand(-1, Envelope("hello", testActorPath))
      whenReady(r) { value =>
        value should be {
          statemachine.AlreadyApplied
        }
      }

      actor.stop()
    }

    "respond with MissingClientActorPath if index is ok but no ActorPath " in {
      val actorRef = TestActorRef[MockRaftStateMachine]
      val actor = actorRef.underlyingActor

      val r = actorRef ? WrappedClientCommand(0, "hello")
      whenReady(r) { value =>
        value should be {
          statemachine.MissingClientActorPath
        }
      }

      actor.stop()
    }


  }
}
