package raft.cluster


import adfs.utils._
import akka.actor.{ActorRef, FSM, Props}
import raft.persistence.Persistence
import raft.statemachine.StateMachine

import scala.concurrent.duration._
import scala.language.{higherKinds, postfixOps}


/**
 * Created by kosii on 2014. 11. 01..
 */

object RaftActor {
  def props[T, D, M <: StateMachine[_, _]](id: Int, clusterConfiguration: ClusterConfiguration, minQuorumSize: Int, persistence: Persistence[T, D], clazz: Class[M], args: Any*): Props = {
    Props(classOf[RaftActor[T, D, M]], id, clusterConfiguration, minQuorumSize, persistence, clazz, args)
  }


  def determineCommitIndex(clusterConfiguration: ClusterConfiguration, matchIndex: Map[Int, Option[Int]]): Option[Int] = {

    // we take the median of the sorted
    val currentMatchIndexMedian = median(
      clusterConfiguration.currentConfig.map(
        i => matchIndex.getOrElse(i._1, None)
      ).toVector.sorted
    )

    val newMatchIndexMedian = median(
      clusterConfiguration.newConfig.map(
        i => matchIndex.getOrElse(i._1, None)
      ).toVector.sorted
    )

    // we should not take into account empty cluster configurations
    (clusterConfiguration.currentConfig.size, clusterConfiguration.newConfig.size) match {
      case (0, 0) => {
        // we should not be here
        // TODO: maybe this function should be in the class so to be able use the logger?
        assert(false)
        None
      }
      case (0, _) => newMatchIndexMedian
      case (_, 0) => currentMatchIndexMedian
      case _ => Seq(currentMatchIndexMedian, newMatchIndexMedian).min
    }
  }
}

/**
 *
 * @param _clusterConfiguration contains the complete list of the cluster members (including self)
 * @param replicationFactor
 */

class RaftActor[T, D, M <: StateMachine[_, _]](id: Int, _clusterConfiguration: ClusterConfiguration, replicationFactor: Int, persistence: Persistence[T, D], clazz: Class[M], args: Any*) extends FSM[Role, Data] {

  def electionTimeout = NormalDistribution.nextGaussian(500, 40) milliseconds
//  def currentTerm = persistence.getCurrentTerm

  val stateMachine = context.actorOf(Props(clazz, args: _*))

  startWith(stateName = Follower, stateData = FollowerState(_clusterConfiguration))

  def isStale(term: Int): Boolean = term < persistence.getCurrentTerm
  def isCurrent(term: Int): Boolean = term == persistence.getCurrentTerm
  def isNew(term: Int): Boolean = persistence.getCurrentTerm < term


  when(Follower, stateTimeout = 2 * electionTimeout) {
    case Event(a: AppendEntries[T], FollowerState(clusterConfiguration, commitIndex, _)) => {
      val AppendEntries(term: Int, leaderId: Int, prevLogIndex: Option[Int], prevLogTerm: Option[Int], entries: Seq[(Either[ReconfigureCluster, T], ActorRef)], leaderCommit: Option[Int]) = a

      if (isStale(term)) {
        stay replying TermExpired(persistence.getCurrentTerm)
      } else {
        if (isNew(term)) {
          persistence.setCurrentTerm(term)
          persistence.clearVotedFor()
        }
        if (persistence.termMatches(prevLogIndex, prevLogTerm)) {
          persistence.appendLog(prevLogIndex, persistence.getCurrentTerm, entries)
          // TODO: check boundaries
          // TODO: how can this work? start should be always None! this means that we never apply here?
          for {
            start <- commitIndex.orElse(Some(-1))
            stop <- leaderCommit
          } {
            // TODO: we should really do something with last applied
            persistence.logsBetween(start + 1, stop + 1).zipWithIndex.foreach({
              case ((Right(command), actorRef), i) => {
                log.info("appying stuff to the statemachine in the follower")
                stateMachine ! (i + start, command)
              }
              case ((Left(ReconfigureCluster(clusterConfiguration)), actorRef), i) => {
                log.warning("We have to change change cluster configuration here!")
              }
            })
          }
          stay using FollowerState(clusterConfiguration = clusterConfiguration, commitIndex = leaderCommit, leaderId = Some(leaderId)) replying LogMatchesUntil(this.id, persistence.lastLogIndex)
        } else {
          stay replying InconsistentLog(this.id)
        }

      }

    }

    case Event(RequestVote(term, candidateId, lastLogIndex, lastLogTerm), FollowerState(clusterConfiguration, commitIndex, _)) => {
      // TODO: check the terms
      if (isStale(term)) {
        stay replying TermExpired(persistence.getCurrentTerm)
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

    case Event(StateTimeout, FollowerState(clusterConfiguration, commitIndex, leaderId)) => {
      println("No heartbeat received since")
      goto(Candidate)
        .using(CandidateState(clusterConfiguration, commitIndex, leaderId, 0))
      //          .forMax(utils.NormalDistribution.nextGaussian(500, 40) milliseconds)
    }

    case Event(t: ClientCommand[T], FollowerState(clusterConfiguration, commitIndex, leaderIdOpt)) => {
      log.warning("I've just received a client command, but I'm no leader!")
      leaderIdOpt match {
        case None => stay replying WrongLeader
        case Some(leaderPath) => stay replying ReferToLeader(leaderPath)
      }
    }

  }


  when(Leader) {
    case Event(t : ClientCommand[T], l@LeaderState(clusterConfiguration, commitIndex, nextIndex, matchIndex)) => {
      val ClientCommand(c) = t
      persistence.appendLog(persistence.getCurrentTerm, Right(c), sender)
      stay using l.copy(nextIndex = nextIndex + (this.id -> Some(persistence.nextIndex)))
    }

    case Event(Tick, l@LeaderState(clusterConfiguration, commitIndex, nextIndexes, matchIndex)) => {
      log.info("It's time to send a heartbeat!!!")
      for {
        (id, path) <- clusterConfiguration.currentConfig ++ clusterConfiguration.newConfig
        if (id != this.id)
      } {
        val prevLogIndex = nextIndexes.getOrElse(id, None) match {
          case Some(i) if (i > 0) => Some(i - 1)
          case _ => None
        }
        val nextIndex = nextIndexes.getOrElse(id, None).getOrElse(0)

        // TODO: when sending heartbeat's we have to send them from the nextIndex and not just an empty `entries` sequence
        // TODO: implemented, need to check
        log.info(s"follower ${id}'s nextIndex: ${nextIndex}")
        log.info("sending data with the heartbeat: " + persistence.logsBetween(nextIndex, persistence.nextIndex))
        context.actorSelection(path) ! AppendEntries(
          persistence.getCurrentTerm, this.id, prevLogIndex, prevLogIndex.flatMap(persistence.getTerm(_)),
          persistence.logsBetween(nextIndex, persistence.nextIndex), commitIndex
        )
      }
      log.info("we have sent so much heartbeat!!!")
      stay using l
    }

    case Event(GrantVote(term), l@LeaderState(clusterConfiguration, commitIndex, nextIndex, matchIndex)) => {
      if (term > persistence.getCurrentTerm) {
        log.info("Received GrantVote for a term in the future, becoming Follower")
        goto(Follower) using FollowerState(clusterConfiguration)
      } else {
        log.info(s"Yo, I'm already the boss, but thanks ${sender}")
        stay
      }
    }

    case Event(l@LogMatchesUntil(id, _matchIndex), LeaderState(clusterConfiguration, commitIndex, nextIndex, matchIndex)) => {
      log.debug(s"${l} received")
      //TODO: verify if it's correct
      val newNextIndex: Map[Int, Option[Int]] = nextIndex + (id -> _matchIndex.map({ i => i + 1}) )
      val newMatchIndex = matchIndex + (id -> _matchIndex)
      val newCommitIndex: Option[Int] = RaftActor.determineCommitIndex(clusterConfiguration, newMatchIndex)
      if (commitIndex != newCommitIndex) {
        for {
          stop <- newCommitIndex
          start <- commitIndex.orElse(Some(-1))
        } yield {
//          val start = commitIndex.getOrElse(-1)
          log.debug(s"leader committing log entries between ${start+1} ${stop+1}")
          persistence.logsBetween(start + 1, stop + 1).zipWithIndex.map({
            case ((Right(command), actorRef), i) =>
              stateMachine.tell((i + start, command), actorRef)
              None
            case ((Left(ReconfigureCluster(clusterConfiguration)), actorRef), i) =>
              Some(clusterConfiguration)
          })
        }

      }
      

      stay using LeaderState(clusterConfiguration, newCommitIndex, newNextIndex, newMatchIndex)
    }

    case Event(i@InconsistentLog(id), LeaderState(clusterConfiguration, commitIndex, nextIndex, matchIndex)) => {
      log.info(s"${i} received")
      val newIndex = nextIndex.getOrElse(id, None) match {
        case Some(index) if (index > 0) => Some(index - 1)
        case _ => None
      }
      stay using LeaderState(clusterConfiguration, commitIndex, nextIndex + (id -> newIndex), matchIndex)
    }

    case Event(Join(_id), LeaderState(clusterConfiguration, commitIndex, nextIndex, matchIndex)) => {

      for {
        (id, path) <- clusterConfiguration.currentConfig
      } {
        context.actorSelection(path) ! ReconfigureCluster(ClusterConfiguration(clusterConfiguration.currentConfig, clusterConfiguration.currentConfig + (_id -> sender.path), None))
      }
      stay replying ReconfigureCluster(ClusterConfiguration(clusterConfiguration.currentConfig, clusterConfiguration.currentConfig + (id -> sender.path), None))
    }

    case Event(RequestVote(term, candidateId, lastLogIndex, lastLogTerm), LeaderState(clusterConfiguration, commitIndex, _, _)) => {
      if (isStale(term)) {
        // NOTE: § 5.1
        stay replying TermExpired(persistence.getCurrentTerm)
      } else if (persistence.lastLogIndex == lastLogIndex && persistence.lastLogTerm == lastLogTerm) {
        if (isNew(term)) {
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
        stay replying InconsistentLog(this.id)
      }
    }

    case Event(Leave(4), LeaderState(clusterConfiguration, commitIndex, nextIndex, matchIndex)) => {

      for {
        (id, path) <- clusterConfiguration.currentConfig
      } {
        context.actorSelection(path) ! ReconfigureCluster(ClusterConfiguration(clusterConfiguration.currentConfig, clusterConfiguration.currentConfig + (id -> sender.path), None))
      }
      stay replying ReconfigureCluster(ClusterConfiguration(clusterConfiguration.currentConfig, clusterConfiguration.currentConfig + (id -> sender.path), None))
    }

  }

  when(Candidate, stateTimeout = NormalDistribution.nextGaussian(500, 40) millis) {
    case Event(GrantVote(term), s: CandidateState) => {
      // TODO: maybe we should check the term?
      val numberOfVotes = s.numberOfVotes + 1
      if (math.max((math.floor(replicationFactor/2) + 1), (math.floor(s.clusterConfiguration.currentConfig.size / 2) + 1)) <= numberOfVotes) {
        // TODO: we have to correctly fill out nextIndex and matchIndex
        goto(Leader) using LeaderState(s.clusterConfiguration, s.commitIndex, Map(), Map())
      } else {
        stay using s.copy(numberOfVotes = numberOfVotes)
      }
    }

    case Event(StateTimeout, CandidateState(clusterConfiguration, commitIndex, leaderId, votes)) => {
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


      goto(Candidate) using CandidateState(clusterConfiguration, commitIndex, leaderId, 0)


    }


    case Event(RequestVote(term, candidateId, lastLogIndex, lastLogTerm), CandidateState(clusterConfiguration, commitIndex, _, _)) => {
      if (isStale(term)) {
        // NOTE: § 5.1
        stay replying TermExpired(persistence.getCurrentTerm)
      } else if (persistence.lastLogIndex == lastLogIndex && persistence.lastLogTerm == lastLogTerm) {
        if (isNew(term)) {
          persistence.clearVotedFor()
          goto(Follower) using FollowerState(clusterConfiguration, commitIndex, None)
        } else {
          stay
        }
      } else {
        stay replying InconsistentLog(this.id)
      }
    }
  }

  def doThisUnlessTermExpired(term:Int)(currentTerm: =>State)(newTerm: =>State = goto(Follower)): State = {
    if (isStale(term)) {
      stay replying TermExpired(persistence.getCurrentTerm)
    } else if (isCurrent(term)) {
      currentTerm
    } else {
      newTerm
    }
  }


  onTransition {

    case Follower -> Candidate =>
      log.info("transition: Follower -> Candidate")
      nextStateData match {
        case CandidateState(clusterConfiguration, commitIndex, leaderId, numberOfVotes) =>
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

        case s@LeaderState(_, _, _, _) =>
          log.error("invalid data in Candidate state: " + s)

        case s@FollowerState(_, _, _) =>
          log.error("invalid data in Candidate state: " + s)
      }

    case Candidate -> Leader => {
      setTimer("heartbeat", Tick, electionTimeout, true)
      log.info("transition: Candidate -> Leader")

    }

    case Candidate -> Follower => {
      log.info("transition: Candidate -> Follower")

    }

    case Leader -> Follower => {
      cancelTimer("heartbeat")
      log.info("transition: Leader -> Follower")

    }

    case Candidate -> Candidate => {
      // NOTE: same state transition emits notification only starting from akka 2.4

      assert(false)

      log.info("transition: Candidate -> Candidate")

      nextStateData match {
        case CandidateState(clusterConfiguration, commitIndex, leaderId, numberOfVotes) =>
          val currentTerm = persistence.incrementAndGetTerm
          for {
            (id, path) <- clusterConfiguration.currentConfig
            if (id != this.id)
          } {
            log.info(s"requesting vote from ${id}")
            context.actorSelection(path) ! RequestVote(currentTerm, id, persistence.lastLogIndex, persistence.lastLogTerm)
          }
          self ! GrantVote(currentTerm)

        case s@LeaderState(_, _, _, _) =>
          log.error("invalid data in Candidate state: " + s)

        case s@FollowerState(_, _, _) =>
          log.error("invalid data in Candidate state: " + s)

      }
    }
  }

  initialize()
}