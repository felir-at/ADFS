package raft.statemachine.test


import akka.actor.ActorSystem
import akka.pattern.ask
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import raft.statemachine
import raft.statemachine._

import scala.concurrent.duration._
import scala.language.postfixOps


class KVStoreSpec(_system: ActorSystem) extends TestKit(_system)
                                              with WordSpecLike with Matchers with BeforeAndAfterAll
                                              with ScalaFutures with ImplicitSender{
//  implicit val system = ActorSystem("MyActorSystem", ConfigFactory.load("test"))
  def this() = this(ActorSystem("MyActorSystem", ConfigFactory.load("test")))


  implicit val timeout = Timeout(3 seconds)

  "A KVStore" should {

    "respond to a Set command" in {
      val actorRef = TestActorRef[KVStore]
      val actor = actorRef.underlyingActor

      assert(actor.stateMachineDurability.getLastApplied == -1)

      actorRef ! WrappedClientCommand(1, Envelope(SetValue("a", 5), self.path))
      expectMsg(statemachine.OK)

      assert(actor.stateMachineDurability.getLastApplied == 1)

      actor.stop()

    }


    "respond to multiple Set commands" in {
      val actorRef = TestActorRef[KVStore]
      val actor = actorRef.underlyingActor

      actorRef ? WrappedClientCommand(1, Envelope(SetValue("a", 5), self.path))
      expectMsg(statemachine.OK)

      actorRef ? WrappedClientCommand(2, Envelope(SetValue("a", 5), self.path))
      expectMsg(statemachine.OK)

      actor.stop()
    }


    "respond to a Get command with None" in {
      val actorRef = TestActorRef[KVStore]
      val actor = actorRef.underlyingActor


      assert(actor.stateMachineDurability.getLastApplied == -1)
//      WrappedClientCommand(1, Envelope(GetValue("a"), self.path))
      actorRef ? WrappedClientCommand(1, Envelope(GetValue("a"), self.path))
      expectMsg(statemachine.OK(None))

      assert(actor.stateMachineDurability.getLastApplied == 1)

      actor.stop()
    }


    "respond to multiple Get's with None's" in {
      val actorRef = TestActorRef[KVStore]
      val actor = actorRef.underlyingActor

      assert(actor.stateMachineDurability.getLastApplied == -1)

      actorRef ? WrappedClientCommand(1, Envelope(GetValue("a"), self.path))
      expectMsg(statemachine.OK(None))

      assert(actor.stateMachineDurability.getLastApplied == 1)

      actorRef ? WrappedClientCommand(2, Envelope(GetValue("a"), self.path))
      expectMsg(statemachine.OK(None))

      assert(actor.stateMachineDurability.getLastApplied == 2)

      actor.stop()
    }


    "after a SetValue respond to a GetValue with the correct value" in {
      val actorRef = TestActorRef[KVStore]
      val actor = actorRef.underlyingActor

      assert(actor.stateMachineDurability.getLastApplied == -1)

      actorRef ? WrappedClientCommand(1, Envelope(SetValue("a", 5), self.path))
      expectMsg(statemachine.OK)

      assert(actor.stateMachineDurability.getLastApplied == 1)

      actorRef ? WrappedClientCommand(2, Envelope(GetValue("a"), self.path))
      expectMsg(statemachine.OK(Some(5)))

      assert(actor.stateMachineDurability.getLastApplied == 2)
      actor.stop()
    }

    "respond with None" in {
      val actorRef = TestActorRef[KVStore]
      val actor = actorRef.underlyingActor

      assert(actor.stateMachineDurability.getLastApplied == -1)
      actorRef ? WrappedClientCommand(1, Envelope(SetValue("a", 5), self.path))
      expectMsg(statemachine.OK)

      assert(actor.stateMachineDurability.getLastApplied == 1)
      actor.stop()
    }

    "correctly update values" in {
      val actorRef = TestActorRef[KVStore]
      val actor = actorRef.underlyingActor

      assert(actor.stateMachineDurability.getLastApplied == -1)

      actorRef ! WrappedClientCommand(1, Envelope(SetValue("a", 5), self.path))

      expectMsg(statemachine.OK)

      assert(actor.stateMachineDurability.getLastApplied == 1)

      actorRef ! WrappedClientCommand(2, Envelope(GetValue("a"), self.path))

      expectMsg(statemachine.OK(Some(5)))

      assert(actor.stateMachineDurability.getLastApplied == 2)

      actorRef ! WrappedClientCommand(3, Envelope(SetValue("a", 10), self.path))
      expectMsg(statemachine.OK)

      assert(actor.stateMachineDurability.getLastApplied == 3)

      actorRef ! WrappedClientCommand(4, Envelope(GetValue("a"), self.path))
      expectMsg(statemachine.OK(Some(10)))

      assert(actor.stateMachineDurability.getLastApplied == 4)

      actor.stop()
    }

    "return OK when deleting non-existing key" in {
      val actorRef = TestActorRef[KVStore]
      val actor = actorRef.underlyingActor

      assert(actor.stateMachineDurability.getLastApplied == -1)

      actorRef ! WrappedClientCommand(1, Envelope(DeleteValue("a"), self.path))
      expectMsg(statemachine.OK)

      assert(actor.stateMachineDurability.getLastApplied == 1)
      actor.stop()
    }

    "return OK when deleting existing key" in {
      val actorRef = TestActorRef[KVStore]
      val actor = actorRef.underlyingActor

      assert(actor.stateMachineDurability.getLastApplied == -1)

      actorRef ! WrappedClientCommand(0, Envelope(SetValue("a", 5), self.path))
      expectMsg(statemachine.OK)

      assert(actor.stateMachineDurability.getLastApplied == 0)

      actorRef ! WrappedClientCommand(1, Envelope(GetValue("a"), self.path))
      expectMsg(statemachine.OK(Some(5)))

      assert(actor.stateMachineDurability.getLastApplied == 1)

      actorRef ! WrappedClientCommand(2, Envelope(DeleteValue("a"), self.path))
      expectMsg(statemachine.OK)

      assert(actor.stateMachineDurability.getLastApplied == 2)

      actor.stop()
    }

    "not accept out of order indexes" in {

      val actorRef = TestActorRef[KVStore]
      val actor = actorRef.underlyingActor

      assert(actor.stateMachineDurability.getLastApplied == -1)

      actorRef ! WrappedClientCommand(1, Envelope(DeleteValue("a"), self.path))
      expectMsg(statemachine.OK)

      assert(actor.stateMachineDurability.getLastApplied == 1)

      actorRef ! WrappedClientCommand(0, Envelope(GetValue("a"), self.path))
      expectMsg(statemachine.AlreadyApplied)

      assert(actor.stateMachineDurability.getLastApplied == 1)

      actor.stop()
    }

    "not overwrite value because of a SetValue with old indexes" in {
      val actorRef = TestActorRef[KVStore]
      val actor = actorRef.underlyingActor

      assert(actor.stateMachineDurability.getLastApplied == -1)

      actorRef ! WrappedClientCommand(0, Envelope(SetValue("a", 5), self.path))
      expectMsg(statemachine.OK)


      assert(actor.stateMachineDurability.getLastApplied == 0)

      actorRef ! WrappedClientCommand(1, Envelope(GetValue("a"), self.path))
      expectMsg(statemachine.OK(Some(5)))

      assert(actor.stateMachineDurability.getLastApplied == 1)


      actorRef ! WrappedClientCommand(1, Envelope(SetValue("a", 10), self.path))
      expectMsg(statemachine.AlreadyApplied)

      assert(actor.stateMachineDurability.getLastApplied == 1)

      actorRef ! WrappedClientCommand(2, Envelope(GetValue("a"), self.path))
      expectMsg(statemachine.OK(Some(5)))
      assert(actor.stateMachineDurability.getLastApplied == 2)

      actor.stop()
    }

    "not delete value because of a DeleteValue with old indexes" in {
      val actorRef = TestActorRef[KVStore]
      val actor = actorRef.underlyingActor

      assert(actor.stateMachineDurability.getLastApplied == -1)

      actorRef ! WrappedClientCommand(0, Envelope(SetValue("a", 5), self.path))
      expectMsg(statemachine.OK)

      assert(actor.stateMachineDurability.getLastApplied == 0)

      actorRef ! WrappedClientCommand(1, Envelope(GetValue("a"), self.path))
      expectMsg(statemachine.OK(Some(5)))

      assert(actor.stateMachineDurability.getLastApplied == 1)


      actorRef ! WrappedClientCommand(0, Envelope(DeleteValue("a"), self.path))
      expectMsg(statemachine.AlreadyApplied)

      assert(actor.stateMachineDurability.getLastApplied == 1)

      actorRef ! WrappedClientCommand(2, Envelope(GetValue("a"), self.path))
      expectMsg(statemachine.OK(Some(5)))

      assert(actor.stateMachineDurability.getLastApplied == 2)

      actor.stop()
    }



  }

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }
}


