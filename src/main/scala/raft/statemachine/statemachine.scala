package raft


import akka.actor.{LoggingFSM, FSM}
/**
 * Created by kosii on 2014. 10. 20..
 */
package object statemachine {


  // TODO: valahogy meg kell csinalni, hogy az automatikusan ide erkezo uzenetek be legyenek csomagolva egy ClientCommand wrappre classba
  //    es az innen elkuldott cuccok pedig egy ClientResponse wrapper classba, anelkul, hogy a traitet implementalo user errol barmit tudna

  trait StateMachine[S, D] {
    self: FSM[S, D] =>

    def lastApplied: Option[Int]
  }


  sealed trait StateName
  case object UniqueState extends StateName

  sealed trait StateData
  case class Data(lastApplied: Option[Int], store: Map[String, Int]) extends StateData

  sealed trait Command
  case class SetValue(index: Int, key: String, value: Int) extends Command
  case class DeleteValue(index: Int, key: String) extends Command
  case class GetValue(key: String) extends Command


  sealed trait Response
  case object OK extends Response
  case class OK(value: Option[Int]) extends Response
  case object RequestOutOfOrder extends Response


  class KVStore extends StateMachine[StateName, StateData] with LoggingFSM[StateName, StateData] {
    startWith(UniqueState, Data(None, Map()))

    when (UniqueState) {
      // TODO: itt igazabol az indexek ellenorzesenel azt kell ellenorizni, hogy az elkuldott parancs a soronkovetkezo indexet tartalmazza-e

      case Event(SetValue(index, key, value), Data(lastApplied, store)) => lastApplied match {
        case Some(lastIndex) if (lastIndex + 1 == index) =>
          stay using (Data(Some(index), store + (key -> value))) replying OK
//          stay replying OK
        case None if (index == 0) =>
          stay using Data(Some(0), store + (key -> value)) replying OK
        case Some(lastIndex) if (lastIndex >= index) =>
          // already applied
          stay replying OK
        case _ =>
          // we can't skip requests
          stay replying RequestOutOfOrder
      }

      case Event(DeleteValue(index, key), Data(lastApplied, store)) => lastApplied match {
        case Some(lastIndex) if (lastIndex + 1 == index) =>
          stay using (Data(Some(index), store - key)) replying OK
        //          stay replying OK
        case None if (index == 0) =>
          stay using Data(Some(0), store - key) replying OK
        case Some(lastIndex) if (lastIndex >= index) =>
          // already applied
          stay replying OK
        case _ =>
          // we can't skip requests
          stay replying RequestOutOfOrder
      }
      case Event(GetValue(key), Data(lastApplied, store)) =>
          stay using (Data(lastApplied, store)) replying OK(store.lift(key))
    }

    override def lastApplied: Option[Int] = {
      stateData match {
        case Data(lastApplied, _) => lastApplied
        case _ => None
      }
    }
  }
}