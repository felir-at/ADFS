import akka.actor.{Props, ActorPath, FSM, Actor}
import akka.util.Timeout
import scala.concurrent.duration._
import scala.language.postfixOps

import scala.language.higherKinds
import scala.util.{Random, Try}

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
  case class CandidateState(commitIndex: Int, lastApplied: Int, leaderId: Option[Int], numberOfVotes: Int) extends Data
  case class LeaderState(nextIndex: Map[Int, Int], matchIndex: Map[Int, Int]) extends Data


  sealed trait RPC
  case class AppendEntries[T](term: Int, leaderId: Int, prevLogIndex: Int, prevLogTerm: Int, entries: Seq[T], leaderCommit: Int) extends RPC
  case class TermExpired(newTerm: Int) extends RPC
  case object InconsistentLog extends RPC

  case class RequestVote(term: Int, candidateId: Int, lastLogIndex: Option[Int], lastLogTerm: Option[Int]) extends RPC
  case object GrantVote extends RPC

  case class ClientCommand[T](t: T) extends RPC
  case class ReferToLeader(leaderId: Int) extends RPC
  case object WrongLeader extends RPC
  case class ClientCommandResults(status: Try[Any]) extends RPC


  /** The FSM whose state are replicated all over our cluster
    *
    * @tparam S
    * @tparam D
    */
  trait RaftFSM[S, D] extends FSM[S, D] {
    /** As D.Ongaro stated here https://groups.google.com/d/msg/raft-dev/KIozjYuq5m0/XsmYAzLpOikJ, lastApplied
      * should be as durable as the state machine
      *
      * @return
      */
    def lastApplied = Int
  }


  object RaftActor {
    def props[T, D](id: Int, clusterConfiguration: Map[Int, ActorPath], minQuorumSize: Int, persistence: Persistence[T, D]): Props = {
      Props(classOf[RaftActor[T, D]], id, clusterConfiguration, minQuorumSize, persistence)
    }
  }

  /**
   *
   * @param clusterConfiguration contains the complete list of the cluster members (including self)
   * @param minQuorumSize
   */
  class RaftActor[T, D](id: Int, clusterConfiguration: Map[Int, ActorPath], minQuorumSize: Int, persistence: Persistence[T, D]) extends FSM[Role, Data] {
    startWith(stateName = Follower, stateData = State(), timeout = Some(utils.NormalDistribution.nextGaussian(500, 40) milliseconds))

    def currentTerm = persistence.getCurrentTerm

    when(Follower) {
      case Event(a: AppendEntries[T], State(commitIndex, lastApplied, _)) => {
        val AppendEntries(term: Int, leaderId: Int, prevLogIndex: Int, prevLogTerm: Int, entries: Seq[T], leaderCommit: Int) = a

        if (term < currentTerm) {
          stay replying TermExpired(currentTerm)
        } else {
          if (term > currentTerm) {
            persistence.setCurrentTerm(term)
            persistence.clearVotedFor()
          }
          if (persistence.getTermAtIndex(prevLogIndex).contains(prevLogTerm)) {
            stay replying InconsistentLog
          } else {
            persistence.appendLog(prevLogIndex, currentTerm, entries)
          }
        }

        stay using State()
      }

      case Event(RequestVote(term, candidateId, lastLogIndex, lastLogTerm), State(commitIndex, lastApplied, _)) => {
        if (term < currentTerm) {
          stay replying TermExpired(currentTerm)
        } else if (persistence.lastLogIndex == lastLogIndex && persistence.lastLogTerm == lastLogTerm)
          persistence.getVotedFor match {
            case None =>
              persistence.setVotedFor(candidateId)
              log.info(s"voting for ${candidateId}")
              stay replying GrantVote
            case Some(id) if (id == candidateId) =>
              log.info(s"voting for ${candidateId}")
              stay replying GrantVote
            case _ =>
              stay
          } else stay
      }

      case Event(StateTimeout, State(commitIndex, lastApplied, leaderId)) => {
        goto(Candidate)
          .using(CandidateState(commitIndex, lastApplied, leaderId, 0))
          .forMax(utils.NormalDistribution.nextGaussian(500, 40) milliseconds)
      }

      case Event(t: ClientCommand[T], State(commitIndex, lastApplied, leaderIdOpt)) => {
        leaderIdOpt match {
          case None => stay replying WrongLeader
          case Some(leaderPath) => stay replying ReferToLeader(leaderPath)
        }
      }

    }

    when(Leader, stateTimeout = 500 milliseconds) {
      case Event(StateTimeout, l@LeaderState(nextIndex, matchIndex)) =>
        log.info("It's time to send a heartbeat!!!")
        stay using l
    }

    when(Candidate, stateTimeout = 500 milliseconds) {
      case Event(GrantVote, s: CandidateState) => {
        val numberOfVotes = s.numberOfVotes + 1
        if ((clusterConfiguration.size / 2 + 1) <= numberOfVotes) {
          goto(Leader) using LeaderState(Map(), Map())
        } else {
          stay using s.copy(numberOfVotes = numberOfVotes)
        }
      }

      case Event(StateTimeout, CandidateState(commitIndex, lastApplied, leaderId, votes)) => {
        log.info(s"vote failed, only ${votes} votes")
        log.info("Candidate -> Candidate")

        val currentTerm = persistence.incrementAndGetTerm
        for {
          (id, path) <- clusterConfiguration
          if (id != this.id)
        } {
          log.info(s"requesting vote from ${id}")
          context.actorSelection(path) ! RequestVote(currentTerm, id, persistence.lastLogIndex, persistence.lastLogTerm)
        }
        self ! GrantVote


        goto(Candidate) using CandidateState(commitIndex, lastApplied, leaderId, 0)


      }

      case Event(RequestVote(term, candidateId, lastLogIndex, lastLogTerm), State(commitIndex, lastApplied, _)) => {
        if (term < currentTerm) {
          // NOTE: § 5.1
          stay replying TermExpired(currentTerm)
        } else if (persistence.lastLogIndex == lastLogIndex && persistence.lastLogTerm == lastLogTerm) {
          if (term > currentTerm) {
            persistence.clearVotedFor()
          }
          persistence.getVotedFor match {
            case None =>
              persistence.setVotedFor(candidateId)
              stay replying GrantVote
            case Some(id) if (id == candidateId) =>
              stay replying GrantVote
            case _ =>
              stay
          }
        } else {
          stay replying InconsistentLog
        }
      }
    }


    onTransition {

      case Follower -> Candidate =>
        log.info("transition: Follower -> Candidate")
        nextStateData match {
          case CandidateState(commitIndex, lastApplied, leaderId, numberOfVotes) =>
            val currentTerm = persistence.incrementAndGetTerm
            for {
              (id, path) <- clusterConfiguration
              if (id != this.id)
            } {
              log.info(s"requesting vote from ${id}")
              context.actorSelection(path) ! RequestVote(currentTerm, id, persistence.lastLogIndex, persistence.lastLogTerm)
            }
            log.info("voting for self")
            self ! GrantVote

          case s@LeaderState(_, _) =>
            log.error("invalid data in Candidate state: " + s)

          case s@State(_, _, _) =>
            log.error("invalid data in Candidate state: " + s)
        }

      case Candidate -> Leader => {
        log.info("transition: Candidate -> Leader")

      }

      case Candidate -> Follower => {
        log.info("transition: Candidate -> Follower")

      }

      case Leader -> Follower => {
        log.info("transition: Leader -> Follower")

      }

      case Candidate -> Candidate => {
        // NOTE: same state transition emits notification only starting from akka 2.4
        log.info("transition: Candidate -> Candidate")


        nextStateData match {
          case CandidateState(commitIndex, lastApplied, leaderId, numberOfVotes) =>
            val currentTerm = persistence.incrementAndGetTerm
            for {
              (id, path) <- clusterConfiguration
              if (id != this.id)
            } {
              log.info(s"requesting vote from ${id}")
              context.actorSelection(path) ! RequestVote(currentTerm, id, persistence.lastLogIndex, persistence.lastLogTerm)
            }
            self ! GrantVote

          case s@LeaderState(_, _) =>
            log.error("invalid data in Candidate state: " + s)

          case s@State(_, _, _) =>
            log.error("invalid data in Candidate state: " + s)

        }
      }
    }

    initialize()
  }


  trait Persistence[T, D] {
    def appendLog(log: T): Unit

    def appendLog(index: Int, term: Int, entries: Seq[T]): Unit

    /** Returns None if log is empty, otherwise returns Some(l), if log is of length l
      *
      */
    def lastLogIndex: Option[Int]

    /** Returns None if log is empty, otherwise returns Some(t), if the term of the last log entry is t
      *
      */
    def lastLogTerm: Option[Int]

    def snapshot: D

    def getTermAtIndex(index: Int): Option[Int]

    def setCurrentTerm(term: Int)

    def getCurrentTerm: Int

    // TODO: this should be atomic or something?
    def incrementAndGetTerm: Int

    def setVotedFor(serverId: Int)

    def getVotedFor: Option[Int]

    def clearVotedFor(): Unit

  }

  case class InMemoryPersistence() extends Persistence[(String, Int), Map[String, Int]] {
    var logs = Vector[(Int, (String, Int))]()
    var currentTerm: Int = 0
    var votedFor: Option[Int] = None

    override def appendLog(log: (String, Int)): Unit = {
      logs = logs :+(getCurrentTerm, log)
      ()
    }

    override def appendLog(index: Int, term: Int, entries: Seq[(String, Int)]): Unit = {
      logs = logs.take(index) ++ entries.map((term, _))
      ()
    }


    override def setCurrentTerm(term: Int) = {
      votedFor = None
      currentTerm = term
    }

    override def getCurrentTerm = currentTerm

    override def snapshot: Map[String, Int] = ???

    override def getVotedFor: Option[Int] = votedFor

    override def setVotedFor(serverId: Int): Unit = {
      votedFor = Some(serverId)
    }

    override def getTermAtIndex(index: Int): Option[Int] = {
      logs.lift(index).map(_._1)
    }

    override def incrementAndGetTerm: Int = this.synchronized {
      currentTerm += 1
      currentTerm
    }

    /** Returns None if log is empty, otherwise returns Some(l-1), if log is of length l
      *
      */
    override def lastLogIndex: Option[Int] = logs.size match {
      case 0 => None
      case s => Some(s - 1)
    }

    /** Returns None if log is empty, otherwise returns Some(t), if the term of the last log entry is t
      *
      */
    override def lastLogTerm: Option[Int] = for {
      i <- lastLogIndex
    } yield {
      logs(i)._1
    }

    override def clearVotedFor(): Unit = {
      votedFor = None
      ()
    }
  }

}

