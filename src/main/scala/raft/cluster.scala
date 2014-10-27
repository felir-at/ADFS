import akka.actor.{Props, ActorPath, FSM, Actor}
import akka.util.Timeout
import raft.persistence.Persistence
import raft.statemachine.StateMachine
import scala.concurrent.duration._
import scala.language.postfixOps

import scala.language.higherKinds
import scala.reflect._
import scala.util.{Random, Try}


/**
 * Created by kosii on 2014.10.04..
 */
package object cluster {

  type ServerId = Int
  type Term = Int

  sealed trait Role
  case object Leader extends Role
  case object Follower extends Role
  case object Candidate extends Role

  case class ClusterConfiguration(currentConfig: Map[Int, ActorPath], newConfig: Map[Int, ActorPath], i: Option[Int])


  sealed trait Data
  case class State(
    clusterConfiguration: ClusterConfiguration,
    commitIndex: Option[Int] = None,
    lastApplied: Option[Int] = None,
    leaderId: Option[Int] = None
  ) extends Data

  case class CandidateState(
    clusterConfiguration: ClusterConfiguration,
    commitIndex: Option[Int],
    lastApplied: Option[Int],
    leaderId: Option[Int],
    numberOfVotes: Int
  ) extends Data

  case class LeaderState(
    clusterConfiguration: ClusterConfiguration,
    commitIndex: Option[Int],
    lastApplied: Option[Int],
    nextIndex: Map[Int, Option[Int]],
    matchIndex: Map[Int, Option[Int]]
  ) extends Data


  sealed trait RPC
  // general messages
  case class TermExpired(newTerm: Int) extends RPC
  case class InconsistentLog(id: Int) extends RPC

  //
  case class AppendEntries[T](term: Int, leaderId: Int, prevLogIndex: Option[Int], prevLogTerm: Option[Int], entries: Seq[T], leaderCommit: Option[Int]) extends RPC
  case class LogMatchesUntil(id: Int, matchIndex: Option[Int]) extends RPC

  // client side communication
  case class ClientCommand[T](t: T) extends RPC
  case class ReferToLeader(leaderId: Int) extends RPC
  case object WrongLeader extends RPC
  case class ClientCommandResults(status: Try[Any]) extends RPC

  // voting protocoll
  case class RequestVote(term: Int, candidateId: Int, lastLogIndex: Option[Int], lastLogTerm: Option[Int]) extends RPC
  case class GrantVote(term: Int) extends RPC

  // cluster management
  case class Join(id: Int) extends RPC
  case class Leave(id: Int) extends RPC
  case class ReconfigureCluster(clusterConfiguration: ClusterConfiguration) extends RPC


  object RaftActor {
    def props[T, D, M <: StateMachine[_, _]](id: Int, clusterConfiguration: ClusterConfiguration, minQuorumSize: Int, persistence: Persistence[T, D], clazz: Class[M], args: Any*): Props = {
      Props(classOf[RaftActor[T, D, M]], id, clusterConfiguration, minQuorumSize, persistence, clazz, args)
    }
  }

  /**
   *
   * @param clusterConfiguration contains the complete list of the cluster members (including self)
   * @param replicationFactor
   */

  class RaftActor[T, D, M <: StateMachine[_, _]](id: Int, clusterConfiguration: ClusterConfiguration, replicationFactor: Int, persistence: Persistence[T, D], clazz: Class[M], args: Any*) extends FSM[Role, Data] {

    def electionTimeout = utils.NormalDistribution.nextGaussian(500, 40) milliseconds
    def currentTerm = persistence.getCurrentTerm

    val stateMachine = context.actorOf(Props(clazz, args: _*))

    startWith(stateName = Follower, stateData = State(clusterConfiguration))

    when(Follower, stateTimeout = 2 * electionTimeout) {
      case Event(a: AppendEntries[T], State(clusterConfiguration, commitIndex, lastApplied, _)) => {
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
            stay using State(clusterConfiguration = clusterConfiguration, commitIndex = leaderCommit, leaderId = Some(leaderId)) replying LogMatchesUntil(this.id, persistence.lastLogIndex)
          } else {
            stay replying InconsistentLog
          }

        }

      }

      case Event(RequestVote(term, candidateId, lastLogIndex, lastLogTerm), State(clusterConfiguration, commitIndex, lastApplied, _)) => {
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

      case Event(StateTimeout, State(clusterConfiguration, commitIndex, lastApplied, leaderId)) => {
        println("No heartbeat received since")
        goto(Candidate)
          .using(CandidateState(clusterConfiguration, commitIndex, lastApplied, leaderId, 0))
//          .forMax(utils.NormalDistribution.nextGaussian(500, 40) milliseconds)
      }

      case Event(t: ClientCommand[T], State(clusterConfiguration, commitIndex, lastApplied, leaderIdOpt)) => {
        leaderIdOpt match {
          case None => stay replying WrongLeader
          case Some(leaderPath) => stay replying ReferToLeader(leaderPath)
        }
      }

    }

    def determineCommitIndex(clusterConfiguration: ClusterConfiguration, matchIndex: Map[Int, Option[Int]]): Option[Int] = ???

    when(Leader, stateTimeout = electionTimeout) {
      case Event(StateTimeout, l@LeaderState(clusterConfiguration, commitIndex, lastApplied, nextIndex, matchIndex)) =>
        log.info("It's time to send a heartbeat!!!")
        for {
          // FIX THIS
          // todo: fix what? :\
          (id, path) <- clusterConfiguration.currentConfig
          if (id != this.id)
        } {
          // TODO: when sending heartbeat's we have to send them from the nextIndex and not just an empty `entries` sequence
          context.actorSelection(path) ! AppendEntries(currentTerm, this.id, persistence.lastLogIndex, persistence.lastLogTerm, Seq(), commitIndex)
        }
        stay using l
      case Event(GrantVote(term), _) => {
        if (term > persistence.getCurrentTerm) {
          log.info("Received GrantVote for a term in the future, becoming Follower")
          goto(Follower) using State(clusterConfiguration)
        } else {

          log.info(s"Yo, I'm already the boss, but thanks ${sender}")
          stay
        }
      }

      case Event(LogMatchesUntil(id, _matchIndex), LeaderState(clusterConfiguration, commitIndex, lastApplied, nextIndex, matchIndex)) => {
        //TODO: verify if it's correct
        val newMatchIndex = matchIndex + (id -> _matchIndex)
        val newCommitIndex:Option[Int] = determineCommitIndex(clusterConfiguration, newMatchIndex)
        stay using LeaderState(clusterConfiguration, newCommitIndex, lastApplied, nextIndex, newMatchIndex)
      }

      case Event(InconsistentLog(id), LeaderState(clusterConfiguration, commitIndex, lastApplied, nextIndex, matchIndex)) => {
        // TODO: we have to decrement the corresponding nextIndex
        // TODO: done, we have to test it
        val newIndex = nextIndex.getOrElse(id, None) match {
          case Some(index) if (index > 0) => Some(index - 1)
          case _ => None
        }
        stay using LeaderState(clusterConfiguration, commitIndex, lastApplied, nextIndex + (id -> newIndex), matchIndex)
      }

      case Event(Join(_id), LeaderState(clusterConfiguration, commitIndex, lastApplied, nextIndex, matchIndex)) => {

        for {
          (id, path) <- clusterConfiguration.currentConfig
        } {
          context.actorSelection(path) ! ReconfigureCluster(ClusterConfiguration(clusterConfiguration.currentConfig, clusterConfiguration.currentConfig + (_id -> sender.path), None))
        }
        stay replying ReconfigureCluster(ClusterConfiguration(clusterConfiguration.currentConfig, clusterConfiguration.currentConfig + (id -> sender.path), None))
      }
    }

    when(Candidate, stateTimeout = utils.NormalDistribution.nextGaussian(500, 40) millis) {
      case Event(GrantVote(term), s: CandidateState) => {
        // TODO: maybe we should check the term?
        val numberOfVotes = s.numberOfVotes + 1
        if (math.max((math.floor(replicationFactor/2) + 1), (math.floor(clusterConfiguration.currentConfig.size / 2) + 1)) <= numberOfVotes) {
          // TODO: we have to correctly fill out nextIndex and matchIndex
          goto(Leader) using LeaderState(s.clusterConfiguration, s.commitIndex, s.lastApplied, Map(), Map())
        } else {
          stay using s.copy(numberOfVotes = numberOfVotes)
        }
      }

      case Event(StateTimeout, CandidateState(clusterConfiguration, commitIndex, lastApplied, leaderId, votes)) => {
        log.info(s"vote failed, only ${votes} votes")
        log.info("Candidate -> Candidate")

        val currentTerm = persistence.incrementAndGetTerm
        for {
          (id, path) <- clusterConfiguration.currentConfig
          if (id != this.id)
        } {
          log.info(s"requesting vote from ${id}")
          context.actorSelection(path) ! RequestVote(currentTerm, this.id, persistence.lastLogIndex, persistence.lastLogTerm)
        }
        self ! GrantVote(currentTerm)


        goto(Candidate) using CandidateState(clusterConfiguration, commitIndex, lastApplied, leaderId, 0)


      }

      case Event(RequestVote(term, candidateId, lastLogIndex, lastLogTerm), State(clusterConfiguration, commitIndex, lastApplied, _)) => {
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
          case CandidateState(clusterConfiguration, commitIndex, lastApplied, leaderId, numberOfVotes) =>
            val currentTerm = persistence.incrementAndGetTerm
            for {
              (id, path) <- clusterConfiguration.currentConfig
              if (id != this.id)
            } {
              log.info(s"requesting vote from ${id}")
              context.actorSelection(path) ! RequestVote(currentTerm, this.id, persistence.lastLogIndex, persistence.lastLogTerm)
            }
            log.info("voting for self")
            self ! GrantVote(currentTerm)

          case s@LeaderState(_, _, _, _, _) =>
            log.error("invalid data in Candidate state: " + s)

          case s@State(_, _, _, _) =>
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
          case CandidateState(clusterConfiguration, commitIndex, lastApplied, leaderId, numberOfVotes) =>
            val currentTerm = persistence.incrementAndGetTerm
            for {
              (id, path) <- clusterConfiguration.currentConfig
              if (id != this.id)
            } {
              log.info(s"requesting vote from ${id}")
              context.actorSelection(path) ! RequestVote(currentTerm, id, persistence.lastLogIndex, persistence.lastLogTerm)
            }
            self ! GrantVote(currentTerm)

          case s@LeaderState(_, _, _, _, _) =>
            log.error("invalid data in Candidate state: " + s)

          case s@State(_, _, _, _) =>
            log.error("invalid data in Candidate state: " + s)

        }
      }
    }

    initialize()
  }


}

