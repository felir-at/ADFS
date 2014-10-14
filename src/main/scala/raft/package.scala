import akka.actor.{ActorPath, FSM, Actor}
import scala.concurrent.duration._

import scala.language.higherKinds
import scala.util.Try

/**
 * Created by kosii on 2014.10.04..
 */
package object raft {

  type ServerId = Int
  type Term = Int

  sealed trait Role
  case object Leader extends Role
  case object Follower extends Role
  case object Candidate extends Role


  sealed trait Data
  case class State(commitIndex: Int = 0, lastApplied: Int = 0, leaderId: Option[Int] = None) extends Data
  case class LeaderState(nextIndex: Map[Int, Int], matchIndex: Map[Int, Int]) extends Data



  sealed trait RPC
  case class AppendEntries[T](term: Int, leaderId: Int, prevLogIndex: Int, prevLogTerm: Int, entries: Seq[T], leaderCommit: Int) extends RPC
  case class AppendEntriesResult(term: Int, succ: Boolean) extends RPC

  case class RequestVote() extends RPC
  case class RequestVoteResult() extends RPC

  case class ClientCommand[T](t: T) extends RPC
  case class ReferToLeader(leaderId: Int) extends RPC
  case object WrongLeader extends RPC
  case class ClientCommandResults(status: Try[Any]) extends RPC


  /**
   *
   * @param actorBuddies
   *                     sdfsdf
   * @param minQuorumSize
   */
  class RaftActor[T, D](actorBuddies: Set[ActorPath], minQuorumSize: Int, persistence: Persistence[T, D]) extends Actor with FSM[Role, Data] {
    startWith(Follower, State())

    when(Follower, stateTimeout = 100 milliseconds) {
      case Event(a: AppendEntries[T], State(commitIndex, lastApplied, _)) => {

        val AppendEntries(term: Int, leaderId: Int, prevLogIndex: Int, prevLogTerm: Int, entries: Seq[T], leaderCommit: Int) = a

        val currentTerm = persistence.getCurrentTerm
        if (term < currentTerm) {
          stay replying false
        } else if (persistence.getTermAt(prevLogIndex) != prevLogTerm){
          stay replying false
        } else {
          persistence.appendLog(prevLogIndex, currentTerm, entries)

        }

        stay using State()
      }
      case Event(StateTimeout, s: State) => {
        goto(Candidate) using s
      }

      case Event(t: T, State(commitIndex, lastApplied, leaderIdOpt))=> {
        leaderIdOpt match {
          case None => stay replying WrongLeader
          case Some(leaderId) => stay replying ReferToLeader(leaderId)
        }
      }

    }

    when(Leader, stateTimeout = 50 milliseconds) {
      case Event(_, _) => ???
    }
  }




  trait Persistence[T, D] {
    def appendLog(log: T): Unit
    def appendLog(index: Int, term: Int, entries: Seq[T]): Unit

    def snapshot: D

    def getTermAt(index: Int): Option[Int]

    def setCurrentTerm(term: Int)
    def getCurrentTerm: Int

    def setVotedFor(serverId: Int)
    def getVotedFor: Option[Int]

  }

  case class InMemoryPersistence() extends Persistence[(String, Int), Map[String, Int]] {
    var logs = Vector[(Int, (String, Int))]()
    var currentTerm: Int = 0
    var votedFor: Option[Int] = None

    override def appendLog(log: (String, Int)): Unit = {
      logs = logs :+ Pair(getCurrentTerm, log)
      ()
    }
    override def appendLog(index: Int, term: Int, entries: Seq[(String, Int)]): Unit = {
      logs = logs.take(index) ++ entries.map((term, _))
      ()
    }



    override def setCurrentTerm(term: Int) = {
      currentTerm = term
    }

    override def getCurrentTerm = currentTerm

    override def snapshot: Map[String, Int] = ???

    override def getVotedFor: Option[Int] = votedFor

    override def setVotedFor(serverId: Int): Unit = { votedFor = Some(serverId) }

    override def getTermAt(index: Int): Option[Int] = {
      logs.lift(index).map(_._1)
    }

  }

}
