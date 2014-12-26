package raft

import java.io._

import akka.actor._


import org.iq80.leveldb._

import org.iq80.leveldb.impl.Iq80DBFactory._
import java.io.File

import raft.cluster.ClientCommand
import scala.math.max

import adfs.utils.StateOps

import scala.pickling._

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
      case msg @ Envelope(command, path) =>
        log.debug(s"in the statemachine ${msg}")
        super.receive(msg)
      case a =>
        log.debug(s"in the statemachine but no actorpath :( ${a}")
        sender ! MissingClientActorPath
    }
  }


  trait RaftStateMachineAdaptor[S, D] extends StateMachine[S, D] {

    val stateMachineDurability: super.T

    abstract override def receive: Receive = {
      case WrappedClientCommand(index, e@Envelope(command, clientPath)) =>
        log.debug(s"forwarding client command ${command} with ${index} from ${clientPath}. current last applied: ${stateMachineDurability.getLastApplied}")
        if (index <= stateMachineDurability.getLastApplied) {
          log.debug("\talready applied, sending back AlreadyApplied")
          // command already applied
          // TODO: send back a message to update the index in the cluster
          sender ! AlreadyApplied(stateMachineDurability.getLastApplied + 1)
        } else {
          log.debug("\tnot yet applied, apply and setLastApplied")
          stateMachineDurability.setLastApplied(index)
          super.receive(e)
        }
      case otherWise => super.receive(otherWise)
    }
  }


  case class AlreadyApplied(nextIndex: Int)
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

  object Command {
    class CommandPickling(implicit val format: PickleFormat) extends SPickler[Command] with Unpickler[Command] {
      override def pickle(picklee: Command, builder: PBuilder): Unit = picklee match {
        case sv@SetValue(key: String, value: Int) =>
          builder.beginEntry(sv)
          builder.putField("type", { b =>
            b.hintTag(FastTypeTag.String).beginEntry("set").endEntry()
          })
          builder.putField("key", { b =>
            b.hintTag(FastTypeTag.String).beginEntry(key).endEntry()
          })
          builder.putField("value", { b =>
            b.hintTag(FastTypeTag.Int).beginEntry(value).endEntry()
          })
          builder.endEntry()
        case gv@GetValue(key) =>
          builder.beginEntry(gv)
          builder.putField("type", { b =>
            b.hintTag(FastTypeTag.String).beginEntry("get").endEntry()
          })
          builder.putField("key", { b =>
            b.hintTag(FastTypeTag.String).beginEntry(key).endEntry()
          })
          builder.endEntry()
        case dv@DeleteValue(key) =>
          builder.beginEntry(dv)
          builder.putField("type", { b =>
            b.hintTag(FastTypeTag.String).beginEntry("delete").endEntry()
          })
          builder.putField("key", { b =>
            b.hintTag(FastTypeTag.String).beginEntry(key).endEntry()
          })
          builder.endEntry()
      }

      override def unpickle(tag: => FastTypeTag[_], reader: PReader): Any = {
        reader.beginEntry()
        val tpe = implicitly[Unpickler[String]].unpickle(
          reader.beginEntry(), reader.readField("type")
        ).asInstanceOf[String]
        reader.endEntry()

        val command: Command = tpe match {
          case "get" =>
            val tag = reader.beginEntry()
            val key = implicitly[Unpickler[String]].unpickle(
              tag, reader.readField("key")
            ).asInstanceOf[String]
            reader.endEntry()
            GetValue(key)
          case "set" =>
            val keyTag = reader.beginEntry()
            val key = implicitly[Unpickler[String]].unpickle(
              keyTag, reader.readField("key")
            ).asInstanceOf[String]
            reader.endEntry()
            val valueTag = reader.beginEntry()
            val value = implicitly[Unpickler[Int]].unpickle(
              keyTag, reader.readField("value")
            ).asInstanceOf[Int]
            reader.endEntry()
            SetValue(key, value)
          case "delete" =>
            val tag = reader.beginEntry()
            val key = implicitly[Unpickler[String]].unpickle(
              tag, reader.readField("key")
            ).asInstanceOf[String]
            reader.endEntry()
            DeleteValue(key)
        }

        reader.endEntry()

        command
      }

    }
  }


  sealed trait Response
  case object OK extends Response
  case class OK(value: Option[Int]) extends Response
  case object RequestOutOfOrder extends Response


  class KVStore extends StateMachine[StateName, StateData] with RaftStateMachineAdaptor[StateName, StateData] with LoggingFSM[StateName, StateData] {
    override type T = InMemoryStateMachine
    override val stateMachineDurability: T = InMemoryStateMachine()

    startWith(UniqueState, Data(Map()))

    when (UniqueState) {
      case Event(Envelope(s@SetValue(key, value), client), Data(store)) =>
        log.info(s"executin ${s}")
        stay using Data(store + (key -> value)) sending(client, OK)
      case Event(Envelope(d@DeleteValue(key), client), Data(store))     =>
        log.info(s"execute ${d}")
        stay using Data(store - key) sending(client, OK)
      case Event(Envelope(g@GetValue(key), client), Data(store))        =>
        log.info(s"execute ${g}")
        stay using Data(store) sending(client, OK(store.lift(key)))
    }
  }

}