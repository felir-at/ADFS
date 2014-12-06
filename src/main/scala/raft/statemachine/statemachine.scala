package raft

import java.io._

import akka.actor.{ActorRef, FSM, LoggingFSM}


import org.iq80.leveldb._

import org.iq80.leveldb.impl.Iq80DBFactory._
import java.io.File

import raft.cluster.ClientCommand
import scala.math.max

/**
 * Created by kosii on 2014. 10. 20..
 */
package object statemachine {


  // TODO: valahogy meg kell csinalni, hogy az automatikusan ide erkezo uzenetek be legyenek csomagolva egy ClientCommand wrappre classba
  //    es az innen elkuldott cuccok pedig egy ClientResponse wrapper classba, anelkul, hogy a traitet implementalo user errol barmit tudna,
  //    ezzel elkerulhetnenk az ilyen hibakat: unhandled event OK(Some(5)) in state Follower

//  case class ClientCommand[S](t: S)
//  case class ClientResponse[S](t: S)
  case class WrappedClientCommand[S](index: Int, command: S)

  sealed trait StateMachineDurability {
    def getLastApplied: Int
    def setLastApplied(index: Int)
  }

  case class InMemoryStateMachine() extends StateMachineDurability {
    var index = -1
    override def getLastApplied: Int = index

    override def setLastApplied(index: Int): Unit = this.index = max(index, getLastApplied)
  }

  case class OnDiskStateMachine(file: File) extends StateMachineDurability {

    override def getLastApplied: Int = try {
      new DataInputStream(new BufferedInputStream(new FileInputStream(file))).readInt()
    } catch {
      case fnfe: FileNotFoundException => -1
      case e: Exception => throw e
    }

    override def setLastApplied(index: Int): Unit = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file))).writeInt(max(index, getLastApplied))

  }

  case class LevelDBStateMachine(db: DB) extends StateMachineDurability {
    override def getLastApplied: Int = {
//      val options: Options = new Options()
//      options.createIfMissing(true)
//      val db: DB = factory.open(new File(dbname), options)

      val res = db.get(bytes("lastApplied"))
      if (res == null) {
        -1
      } else {
        asString(res).toInt
      }
    }

    override def setLastApplied(index: Int): Unit = {

//      val options: Options = new Options()
//      options.createIfMissing(true)
//      val db: DB = factory.open(new File(dbname), options)

      val lastApplied = getLastApplied
      db.put(bytes("lastApplied"), bytes(max(index, lastApplied).toString))
    }
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

    val stateMachineDurability: selfRef.T

    override def receive = {
      case WrappedClientCommand(index, command) => {
        println(s"forwarding cliend command ${command} with ${index}")
        if (index <= stateMachineDurability.getLastApplied) {
          // command already applied
        } else {
          self forward command
//          self.tell(command, sender())
          stateMachineDurability.setLastApplied(index)
        }
      }
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
//    def lastApplied: Option[Int]
    type T <: StateMachineDurability
  }


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
//    type StateMachineDurability = InMemoryStateMachine

    override type T = InMemoryStateMachine
    override val stateMachineDurability: T = InMemoryStateMachine()

    startWith(UniqueState, Data(Map()))

    when (UniqueState) {
      // TODO: itt igazabol az indexek ellenorzesenel azt kell ellenorizni, hogy az elkuldott parancs a soronkovetkezo indexet tartalmazza-e
      // TODO: mivel a cluster management parancsok miatt lehetnek kimarado indexek, emiatt azt kell ellenorizni, hogy az indexek szigoruan monoton nonek-e
      // TODO: de amugy az indexeket ebben a layerben nem kene mutatni (trello: http://goo.gl/SJ6gzL)

      case Event(SetValue(key, value), Data(store)) => /*lastApplied match*/ {
        stay using Data(store + (key -> value)) replying OK
//        case Some(lastIndex) if (lastIndex < index) =>
//        case None =>
//          stay using Data(Some(index), store + (key -> value)) replying OK
//        case _ =>
//          // already applied
//          stay replying OK
      }

      case Event(DeleteValue(key), Data(store)) =>
        stay using Data(store - key) replying OK
//        lastApplied match {
//        case Some(lastIndex) if (lastIndex < index) =>
//          stay using Data(Some(index), store - key) replying OK
//        case None =>
//          stay using Data(Some(index), store - key) replying OK
//        case _ =>
//          stay replying OK
//      }
      case Event(GetValue(key), Data(store)) =>
        stay using Data( store) replying OK(store.lift(key))

//        lastApplied match {
//          case Some(lastIndex) if (lastIndex < index) =>
//            stay using Data(Some(index), store) replying OK(store.lift(key))
//          case None =>
//            stay using Data(Some(index), store) replying OK(store.lift(key))
//          case _ =>
//            stay replying RequestOutOfOrder
//        }
//          stay using Data(Some(index), store) replying OK(store.lift(key))
    }

//    override def lastApplied: Option[Int] = {
//      stateData match {
//        case Data(lastApplied, _) => lastApplied
//        case _ => None
//      }
//    }

  }
}