package raft

import akka.actor.{FSM, LoggingFSM}
/**
 * Created by kosii on 2014. 10. 20..
 */
package object statemachine {


  // TODO: valahogy meg kell csinalni, hogy az automatikusan ide erkezo uzenetek be legyenek csomagolva egy ClientCommand wrappre classba
  //    es az innen elkuldott cuccok pedig egy ClientResponse wrapper classba, anelkul, hogy a traitet implementalo user errol barmit tudna



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
    startWith(UniqueState, Data(None, Map()))

    when (UniqueState) {
      // TODO: itt igazabol az indexek ellenorzesenel azt kell ellenorizni, hogy az elkuldott parancs a soronkovetkezo indexet tartalmazza-e
      // TODO: mivel a cluster management parancsok miatt lehetnek kimarado indexek, emiatt azt kell ellenorizni, hogy az indexek szigoruan monoton nonek-e
      // TODO: de amugy az indexeket ebben a layerben nem kene mutatni.

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