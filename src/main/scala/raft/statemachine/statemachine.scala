package raft

import java.io._

import akka.actor._


import org.iq80.leveldb._

import org.iq80.leveldb.impl.Iq80DBFactory._
import java.io.File

import raft.cluster.ClientCommand
import scala.math.max

import adfs.utils.StateOps

/**
 * Created by kosii on 2014. 10. 20..
 */
package object statemachine {


  // TODO: valahogy meg kell csinalni, hogy az automatikusan ide erkezo uzenetek be legyenek csomagolva egy ClientCommand wrappre classba
  //    es az innen elkuldott cuccok pedig egy ClientResponse wrapper classba, anelkul, hogy a traitet implementalo user errol barmit tudna,
  //    ezzel elkerulhetnenk az ilyen hibakat: unhandled event OK(Some(5)) in state Follower

  // NOTE: why wrap command to unwrap them?

  case class WrappedClientCommand[S](index: Int, command: S)

  /** The FSM whose state are replicated all over our cluster
    *
    * @tparam S state name
    * @tparam D state data
    */
  trait StateMachine[S, D] extends FSM[S, D] {
//    selfF: FSM[S, D] =>

    /** As D.Ongaro stated here https://groups.google.com/d/msg/raft-dev/KIozjYuq5m0/XsmYAzLpOikJ, lastApplied
      * should be as durable as the state machine
      *
      * @return
      */
    //    def lastApplied: Option[Int]
    type T <: StateMachineDurability

    override def receive: Receive = {
      case msg @ (command, path: ActorPath) =>
        println(s"in the statemachine ${msg}")
        super.receive(msg)
      case a =>
        println(s"in the statemachine but no actorpath :( ${a}")
        sender ! MissingClientActorPath
    }
  }


  trait RaftStateMachineAdaptor[S, D] extends StateMachine[S, D] {

    val stateMachineDurability: super.T

    abstract override def receive: Receive = {
      case WrappedClientCommand(index, command) => {
        println(s"forwarding cliend command ${command} with ${index}. current last applied: ${stateMachineDurability.getLastApplied}")
        if (index <= stateMachineDurability.getLastApplied) {
          println("\talready applied, sending back AlreadyApplied")
          // command already applied
          // TODO: send back a message to update the index in the cluster
          sender ! AlreadyApplied
        } else {
          println("\tnot yet applied, apply and setLastApplied")
          self forward command
//          self.tell(command, sender())
          stateMachineDurability.setLastApplied(index)
        }
      }
      case otherWise => super.receive(otherWise)
    }
  }


  object AlreadyApplied
  object MissingClientActorPath


  case class Envelope[T](t: T, client: ActorPath)

  sealed trait StateName
  case object UniqueState extends StateName

  sealed trait StateData
  case class Data(store: Map[String, Int]) extends StateData

  sealed trait Command
  case class SetValue(key: String, value: Int) extends Command
  case class DeleteValue(key: String) extends Command
  case class GetValue(key: String) extends Command


  sealed trait Response
  case object OK extends Response
  case class OK(value: Option[Int]) extends Response
  case object RequestOutOfOrder extends Response


  class KVStore extends StateMachine[StateName, StateData] with RaftStateMachineAdaptor[StateName, StateData] with LoggingFSM[StateName, StateData] {
    override type T = InMemoryStateMachine
    override val stateMachineDurability: T = InMemoryStateMachine()

    startWith(UniqueState, Data(Map()))

    when (UniqueState) {
      case Event(Envelope(SetValue(key, value), client), Data(store)) => stay using Data(store + (key -> value)) sending(client, OK)
      case Event(Envelope(DeleteValue(key), client), Data(store))     => stay using Data(store - key) sending(client, OK)
      case Event(Envelope(GetValue(key), client), Data(store))        => stay using Data(store) sending(client, OK(store.lift(key)))
    }
  }

}