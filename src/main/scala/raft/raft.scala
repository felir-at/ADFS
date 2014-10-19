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
  case class State(commitIndex: Option[Int] = None, lastApplied: Option[Int] = None, leaderId: Option[Int] = None) extends Data
  case class CandidateState(commitIndex: Option[Int], lastApplied: Option[Int], leaderId: Option[Int], numberOfVotes: Int) extends Data
  case class LeaderState(commitIndex: Option[Int], lastApplied: Option[Int], nextIndex: Map[Int, Int], matchIndex: Map[Int, Int]) extends Data


  sealed trait RPC
  case class AppendEntries[T](term: Int, leaderId: Int, prevLogIndex: Option[Int], prevLogTerm: Option[Int], entries: Seq[T], leaderCommit: Option[Int]) extends RPC
  case class TermExpired(newTerm: Int) extends RPC
  case object InconsistentLog extends RPC
  case class LogMatchesUntil(id: Int, matchIndex: Option[Int]) extends RPC

  case class RequestVote(term: Int, candidateId: Int, lastLogIndex: Option[Int], lastLogTerm: Option[Int]) extends RPC
  case class GrantVote(term: Int) extends RPC

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
   * @param replicationFactor
   */
  class RaftActor[T, D](id: Int, clusterConfiguration: Map[Int, ActorPath], replicationFactor: Int, persistence: Persistence[T, D]) extends FSM[Role, Data] {

    def electionTimeout = utils.NormalDistribution.nextGaussian(500, 40) milliseconds
    def currentTerm = persistence.getCurrentTerm


    //TODO: we should remove timout,and keep it only for the when(Follower) {...} block
    startWith(stateName = Follower, stateData = State())

    when(Follower, stateTimeout = 2 * electionTimeout) {
      case Event(a: AppendEntries[T], State(commitIndex, lastApplied, _)) => {
        val AppendEntries(term: Int, leaderId: Int, prevLogIndex: Option[Int], prevLogTerm: Option[Int], entries: Seq[T], leaderCommit: Option[Int]) = a

        if (term < currentTerm) {
          stay replying TermExpired(currentTerm)
        } else {
          if (term > currentTerm) {
            persistence.setCurrentTerm(term)
            persistence.clearVotedFor()
          }
          if (persistence.termMatches(prevLogIndex, prevLogTerm)) {
            persistence.appendLog(prevLogIndex, persistence.getCurrentTerm, entries)
            stay using State(commitIndex = leaderCommit, leaderId = Some(leaderId)) replying LogMatchesUntil(this.id, persistence.lastLogIndex)
          } else {
            stay replying InconsistentLog
          }

        }

      }

      case Event(RequestVote(term, candidateId, lastLogIndex, lastLogTerm), State(commitIndex, lastApplied, _)) => {
        if (term < currentTerm) {
          stay replying TermExpired(currentTerm)
        } else if (persistence.termMatches(lastLogIndex, lastLogIndex)) {
          persistence.getVotedFor match {
            case None =>
              persistence.setVotedFor(candidateId)
              log.info(s"not yet voted, so voting for ${candidateId}")
              stay replying GrantVote(term)
            case Some(id) if (id == candidateId) =>
              log.info(s"already voted for ${candidateId}, but voting again for ${candidateId}")
              stay replying GrantVote(term)
            case _ =>
              stay
          }
        } else stay
      }

      case Event(StateTimeout, State(commitIndex, lastApplied, leaderId)) => {
        println("No heartbeat received since")
        goto(Candidate)
          .using(CandidateState(commitIndex, lastApplied, leaderId, 0))
//          .forMax(utils.NormalDistribution.nextGaussian(500, 40) milliseconds)
      }

      case Event(t: ClientCommand[T], State(commitIndex, lastApplied, leaderIdOpt)) => {
        leaderIdOpt match {
          case None => stay replying WrongLeader
          case Some(leaderPath) => stay replying ReferToLeader(leaderPath)
        }
      }

    }

    when(Leader, stateTimeout = electionTimeout) {
      case Event(StateTimeout, l@LeaderState(commitIndex, lastApplied, nextIndex, matchIndex)) =>
        log.info("It's time to send a heartbeat!!!")
        for {
          (id, path) <- clusterConfiguration
          if (id != this.id)
        } {
          context.actorSelection(path) ! AppendEntries(currentTerm, this.id, persistence.lastLogIndex, persistence.lastLogTerm, Seq(), commitIndex)
        }
        stay using l
      case Event(GrantVote(term), _) => {
        if (term > persistence.getCurrentTerm) {
          log.info("Received GrantVote for a term in the future, becoming Follower")
          goto(Follower) using State()
        } else {

          log.info(s"Yo, I'm already the boss, but thanks ${sender}")
          stay
        }
      }

      //TODO: we should update matchIndex
      case Event(LogMatchesUntil(id, mmatchIndex), LeaderState(commitIndex, lastApplied, nextIndex, matchIndex)) => {
        stay
      }

    }

    when(Candidate, stateTimeout = utils.NormalDistribution.nextGaussian(500, 40) millis) {
      case Event(GrantVote(term), s: CandidateState) => {
        // TODO: maybe we should check the term?
        val numberOfVotes = s.numberOfVotes + 1
        if (math.max((math.floor(replicationFactor/2) + 1), (math.floor(clusterConfiguration.size / 2) + 1)) <= numberOfVotes) {
          goto(Leader) using LeaderState(s.commitIndex, s.lastApplied, Map(), Map())
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
          context.actorSelection(path) ! RequestVote(currentTerm, this.id, persistence.lastLogIndex, persistence.lastLogTerm)
        }
        self ! GrantVote(currentTerm)


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
              stay replying GrantVote(term)
            case Some(id) if (id == candidateId) =>
              stay replying GrantVote(term)
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
              context.actorSelection(path) ! RequestVote(currentTerm, this.id, persistence.lastLogIndex, persistence.lastLogTerm)
            }
            log.info("voting for self")
            self ! GrantVote(currentTerm)

          case s@LeaderState(_, _, _, _) =>
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
            self ! GrantVote(currentTerm)

          case s@LeaderState(_, _, _, _) =>
            log.error("invalid data in Candidate state: " + s)

          case s@State(_, _, _) =>
            log.error("invalid data in Candidate state: " + s)

        }
      }
    }

    initialize()
  }


  trait Persistence[T, D] {

    def appendLog(prevLogIndex: Option[Int], term: Int, entries: Seq[T]): Unit

    /** Returns None if log is empty, otherwise returns Some(l), if log is of length l
      *
      */
    def lastLogIndex: Option[Int]

    /** Returns None if log is empty, otherwise returns Some(t), if the term of the last log entry is t
      *
      */
    def lastLogTerm: Option[Int]

    def termMatches(prevLogIndex: Option[Int], prevLogTerm: Option[Int]): Boolean

    def snapshot: D

    def getTerm(index: Int): Option[Int]

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

    override def appendLog(prevLogIndex: Option[Int], term: Int, entries: Seq[(String, Int)]): Unit = prevLogIndex match {
      case None => logs = entries.map((term, _)).toVector
      case Some(index) => logs = logs.take(index + 1) ++ entries.map((term, _))
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

    override def getTerm(index: Int): Option[Int] = {
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

    override def termMatches(prevLogIndex: Option[Int], prevLogTerm: Option[Int]): Boolean = prevLogIndex match {
      case None => true
      case Some(index) =>  prevLogTerm == getTerm(index)
    }
  }

}

