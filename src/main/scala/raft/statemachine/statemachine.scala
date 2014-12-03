package raft

import akka.actor.{ActorRef, FSM, LoggingFSM}
import raft.cluster.ClientCommand

/**
 * Created by kosii on 2014. 10. 20..
 */
package object statemachine {


  // TODO: valahogy meg kell csinalni, hogy az automatikusan ide erkezo uzenetek be legyenek csomagolva egy ClientCommand wrappre classba
  //    es az innen elkuldott cuccok pedig egy ClientResponse wrapper classba, anelkul, hogy a traitet implementalo user errol barmit tudna,
  //    ezzel elkerulhetnenk az ilyen hibakat: unhandled event OK(Some(5)) in state Follower

//  case class ClientCommand[S](t: S)
//  case class ClientResponse[S](t: S)
  case class AAA[S](index: Int, command: S)

  sealed trait StateMachineDurability {
    def getLastApplied: Int
    def setLastApplied(index: Int)
  }
  case class InMemoryStateMachine() extends StateMachineDurability {
    var index = 0
    override def getLastApplied: Int = index

    override def setLastApplied(index: Int): Unit = this.index = index
  }
  case class OnDiskStateMachine() extends StateMachineDurability {
    override def getLastApplied: Int = ???

    override def setLastApplied(index: Int): Unit = ???
  }

//  trait StateMachineModule {
//    trait StateMachineHandler {
//      val stateMachine: ActorRef
//      val stateMachineDurability: StateMachineDurability
//    }
//    type T <: StateMachineHandler
//
//    val handler: T
//  }

  trait RaftStateMachineAdaptor[S, D] extends FSM[S, D] {
    selfRef: StateMachine[S, D] =>

    val stateMachineDurability: selfRef.StateMachineDurability

    override def receive = {
      case AAA(index, command) => self.tell(command, sender())
      case otherWise => super.receive(otherWise)
    }
  }

  /** The FSM whose state are replicated all over our cluster
    *
    * @tparam S state name
    * @tparam D state data
    */
  trait StateMachine[S, D] {
    self: FSM[S, D] =>

    /** As D.Ongaro stated here https://groups.google.com/d/msg/raft-dev/KIozjYuq5m0/XsmYAzLpOikJ, lastApplied
      * should be as durable as the state machine
      *
      * @return
      */
    def lastApplied: Option[Int]
    type StateMachineDurability <: StateMachineDurability
  }


  sealed trait StateName
  case object UniqueState extends StateName

  sealed trait StateData
  case class Data(lastApplied: Option[Int], store: Map[String, Int]) extends StateData

  sealed trait Command
  case class SetValue(key: String, value: Int) extends Command
  case class DeleteValue(key: String) extends Command
  case class GetValue(key: String) extends Command


  sealed trait Response
  case object OK extends Response
  case class OK(value: Option[Int]) extends Response
  case object RequestOutOfOrder extends Response


  class KVStore extends StateMachine[StateName, StateData] with LoggingFSM[StateName, StateData] {
    type StateMachineDurability = InMemoryStateMachine

    startWith(UniqueState, Data(None, Map()))

    when (UniqueState) {
      // TODO: itt igazabol az indexek ellenorzesenel azt kell ellenorizni, hogy az elkuldott parancs a soronkovetkezo indexet tartalmazza-e
      // TODO: mivel a cluster management parancsok miatt lehetnek kimarado indexek, emiatt azt kell ellenorizni, hogy az indexek szigoruan monoton nonek-e
      // TODO: de amugy az indexeket ebben a layerben nem kene mutatni (trello: http://goo.gl/SJ6gzL)

      case Event((index: Int, SetValue(key, value)), Data(lastApplied, store)) => lastApplied match {
        case Some(lastIndex) if (lastIndex < index) =>
          stay using Data(Some(index), store + (key -> value)) replying OK
        case None =>
          stay using Data(Some(index), store + (key -> value)) replying OK
        case _ =>
          // already applied
          stay replying OK
      }

      case Event((index: Int, DeleteValue(key)), Data(lastApplied, store)) => lastApplied match {
        case Some(lastIndex) if (lastIndex < index) =>
          stay using Data(Some(index), store - key) replying OK
        case None =>
          stay using Data(Some(index), store - key) replying OK
        case _ =>
          stay replying OK
      }
      case Event((index: Int, GetValue(key)), Data(lastApplied, store)) => lastApplied match {
        case Some(lastIndex) if (lastIndex < index) =>
          stay using Data(Some(index), store) replying OK(store.lift(key))
        case None =>
          stay using Data(Some(index), store) replying OK(store.lift(key))
        case _ =>
          stay replying RequestOutOfOrder
      }
//          stay using Data(Some(index), store) replying OK(store.lift(key))
    }

    override def lastApplied: Option[Int] = {
      stateData match {
        case Data(lastApplied, _) => lastApplied
        case _ => None
      }
    }
  }
}