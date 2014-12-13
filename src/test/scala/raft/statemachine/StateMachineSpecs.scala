package raft.statemachine

import akka.pattern.{AskTimeoutException, ask}
import akka.util.Timeout
import raft.statemachine.StateMachineSpecs.MockStateMachine
import scala.concurrent.duration._
import akka.actor.{ActorPath, FSM, ActorSystem}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import raft.statemachine

/**
 * Created by kosii on 14. 12. 07..
 */

object StateMachineSpecs {

  class MockStateMachine extends FSM[Any, Any] with StateMachine[Any, Any] {
    startWith(None, None)

    when(None) {
      case Event(_, None) => stay replying None
    }

    initialize()
    override type T = InMemoryStateMachine
//    override val stateMachineDurability: T = InMemoryStateMachine()
  }

}
class StateMachineSpecs(_system: ActorSystem) extends TestKit(_system) with WordSpecLike with Matchers with BeforeAndAfterAll with ScalaFutures with ImplicitSender{
  //  implicit val system = ActorSystem("MyActorSystem", ConfigFactory.load("test"))
  def this() = this(ActorSystem("MyActorSystem", ConfigFactory.load("test")))

  implicit val timeout = Timeout(100 milliseconds)
  val testActorPath = ActorPath.fromString("akka.tcp://system/user/a")

  "A StateMachine" should {

    "respond with MissingClientActorPath if there is no client actor path" in {
      val actorRef = TestActorRef[MockStateMachine]
      val actor = actorRef.underlyingActor

      actorRef ! "hello"
      expectMsg(statemachine.MissingClientActorPath)

      actor.stop()
    }

    "respond with MissingClientActorPath if there is no client actor path even if it's wrapped" in {
      val actorRef = TestActorRef[MockStateMachine]
      val actor = actorRef.underlyingActor

      actorRef ! WrappedClientCommand(0, "hello")
      expectMsg(statemachine.MissingClientActorPath)

      actor.stop()
    }

    "respond with MissingClientActorPath if there is no client actor path even if it's wrapped unless the index is already applied" in {
      val actorRef = TestActorRef[MockStateMachine]
      val actor = actorRef.underlyingActor

      actorRef ! WrappedClientCommand(-2, "hello")
      expectMsg(statemachine.MissingClientActorPath)

      actor.stop()
    }

    "send message to statemachine if there is a client ActorPath" in {
      val actorRef = TestActorRef[MockStateMachine]
      val actor = actorRef.underlyingActor

      actorRef ! Envelope("hello", self.path)
      expectMsg(None)

      actor.stop()

    }
  }
}
