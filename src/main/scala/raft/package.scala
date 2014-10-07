import akka.actor.{ActorPath, FSM, Actor}
import scala.language.higherKinds

/**
 * Created by kosii on 2014.10.04..
 */
package object raft {

  type ServerId = Int
  type Term = Int

  sealed trait State
  case class Leader(currentTerm: Int, votedFor: Int) extends State
  case class Follower() extends State
  case class Candidate() extends State


  sealed trait Data
  case object Uninitialized extends Data
  case class VolatileState(commitIndex: Int, lastApplied: Int) extends Data
  case class LeaderVolatileState() extends Data

  sealed trait RPC
  case class AppendEntries() extends RPC
  case class AppendEntriesResult() extends RPC

  case class RequestVote() extends RPC
  case class RequestVoteResult() extends RPC


  /**
   *
   * @param actorBuddies
   *                     sdfsdf
   * @param minQuorumSize
   */
  class RaftActor[T, D](actorBuddies: Set[ActorPath], minQuorumSize: Int, persistence: Persistence[T, D]) extends Actor with FSM[State, Data] {
    startWith(Follower(), Uninitialized)
  }




  trait Persistence[T, D] {
    def appendLog(log: T): Unit
    def snapshot: D

    def setCurrentTerm(term: Int)
    def getCurrentTerm: Int

    def setVotedFor(serverId: Int)
    def getVotedFor: Option[Int]

  }

  case class InMemoryPersistence() extends Persistence[Tuple2[String, Int], Map[String, Int]] {
    var logs = Vector[Tuple2[Int, Tuple2[String, Int]]]()
    var currentTerm: Int = 0
    var votedFor: Option[Int] = None

    override def appendLog(log: Tuple2[String, Int]): Unit = {
      logs = logs :+ Pair(getCurrentTerm, log)
      ()
    }

    override def setCurrentTerm(term: Int) = {
      currentTerm = term
    }

    override def getCurrentTerm = currentTerm

    override def snapshot: Map[String, Int] = ???

    override def getVotedFor: Option[Int] = votedFor

    override def setVotedFor(serverId: Int): Unit = { votedFor = Some(serverId) }
  }

}
