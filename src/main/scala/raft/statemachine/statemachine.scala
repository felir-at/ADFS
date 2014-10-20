package raft


import akka.actor.FSM
/**
 * Created by kosii on 2014. 10. 20..
 */
package object statemachine {

  trait StateMachine[S, D] extends FSM[S, D] {

    override sealed def whenUnhandled {
    }
  }


  case class S
  case class Data(store: Map[Int, String])

  trait Commands

  class KVStore extends StateMachine[S, Data] with FSM[S, Data] {
    startWith(S, Data(Map()))

    when (S) {
      case ()
    }

  }
}