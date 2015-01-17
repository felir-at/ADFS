package raft.cluster


import adfs.utils._
import akka.actor.FSM.Failure
import akka.actor._
import raft.persistence.Persistence
import raft.statemachine._

import scala.concurrent.duration._
import scala.language.{higherKinds, postfixOps}


/**
 * Created by kosii on 2014. 11. 01..
 */

object RaftActor {
  def props[T, D, M <: RaftStateMachineAdaptor[_, _]](id: Int, clusterConfiguration: ClusterConfiguration, minQuorumSize: Int, persistence: Persistence[T, D], clazz: Class[M], args: Any*): Props = {
    Props(classOf[RaftActor[T, D, M]], id, clusterConfiguration, minQuorumSize, persistence, clazz, args)
  }

  // the commit index is the lower median of the matchIndexes
  def determineCommitIndex(clusterConfiguration: ClusterConfiguration, matchIndex: Map[Int, Option[Int]]): Option[Int] = {

    // we take the median of the sorted
    val currentMatchIndexMedian = median(
      clusterConfiguration.currentConfig.map(
        i => matchIndex.getOrElse(i._1, None)
      ).toVector.sorted
    )

    val newMatchIndexMedian = median(
      clusterConfiguration.newConfig.getOrElse(Map()).map(
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

case class RaftActor[T, D, M <: RaftStateMachineAdaptor[_, _]](id: Int, _clusterConfiguration: ClusterConfiguration, replicationFactor: Int, persistence: Persistence[T, D], clazz: Class[M], args: Any*) extends FSM[Role, ClusterState] {

  def electionTimeout = NormalDistribution.nextGaussian(500, 40) milliseconds
//  def currentTerm = persistence.getCurrentTerm

  // TODO: nem a raft actornak kene peldanyositania, mert igy szar lesz a supervision hierarchy, vagy cake pattern kell, vagy pedig siman atadni
  val stateMachine = context.actorOf(Props(clazz, args: _*))

  startWith(stateName = Follower, stateData = FollowerState(_clusterConfiguration, None))
  setElectionTimer()
  setLivenessTimer()

  def isStale(term: Int): Boolean = term < persistence.getCurrentTerm
  def isCurrent(term: Int): Boolean = term == persistence.getCurrentTerm
  def isNew(term: Int): Boolean = persistence.getCurrentTerm < term


  when(Follower) {

//    case Event(StateTimeout, FollowerState(clusterConfiguration, commitIndex, leaderId)) => {
//      println("No heartbeat received since")
//      goto(Candidate)
//        .using(CandidateState(clusterConfiguration, commitIndex, leaderId, 0))
//      //          .forMax(utils.NormalDistribution.nextGaussian(500, 40) milliseconds)
//    }

    case Event(ElectionTimeout, FollowerState(clusterConfiguration, commitIndex, leaderId)) => {
      log.info(s"ElectionTimeout in Candidate state in term ${persistence.getCurrentTerm}")
      goto(Candidate)
        .using(CandidateState(clusterConfiguration, commitIndex, leaderId, 0))
    }

    case Event(t: ClientCommand[T], FollowerState(clusterConfiguration, commitIndex, leaderIdOpt)) => {
      log.warning("I've just received a client command, but I'm no leader!")
      leaderIdOpt match {
        case None => stay replying WrongLeader
        case Some(leaderPath) => stay replying ReferToLeader(leaderPath)
      }
    }

  }


  when(Candidate) {
    case Event(GrantVote(term), s: CandidateState) => {
      val currentTerm = persistence.getCurrentTerm
      if (isStale(term)) {
        stay replying TermExpired(currentTerm)
      } else if (isCurrent(term)) {
        val numberOfVotes = s.numberOfVotes + 1
        if (math.max((math.floor(replicationFactor/2) + 1), (math.floor(s.clusterConfiguration.currentConfig.size / 2) + 1)) <= numberOfVotes) {
          // TODO: we have to correctly fill out nextIndex and matchIndex
          goto(Leader) using LeaderState(s.clusterConfiguration, s.commitIndex, Map(), Map())
        } else {
          stay using s.copy(numberOfVotes = numberOfVotes)
        }
      } else {
        log.info("WTF??!!? somebody granting votes for a future term! it's just crazy")
        goto(Follower) using FollowerState(s.clusterConfiguration, s.commitIndex, None)
      }
    }

    case Event(ElectionTimeout, CandidateState(clusterConfiguration, commitIndex, leaderId, votes)) => {
      log.info(s"vote failed, only ${votes} votes")
      log.info("Candidate -> Candidate")


      log.info("launching increment step")
      val currentTerm = persistence.incrementAndGetTerm
      log.info("increment ended")
      for {
        (id, path) <- clusterConfiguration.currentConfig
        if (id != this.id)
      } {
        log.info(s"requesting vote from ${id} for term ${currentTerm}")
        context.actorSelection(path) ! RequestVote(currentTerm, this.id, persistence.lastLogIndex, persistence.lastLogTerm)
      }
      persistence.setVotedFor(this.id)
      self ! GrantVote(currentTerm)


      goto(Candidate) using CandidateState(clusterConfiguration, commitIndex, leaderId, 0)


    }
  }

  def peers(clusterConfiguration: ClusterConfiguration): Map[Int, ActorPath] = clusterConfiguration.currentConfig ++ clusterConfiguration.newConfig.getOrElse(Map()) filter { case (id, path) => id != this.id}


  when(Leader) {
    case Event(t : ClientCommand[T], l@LeaderState(clusterConfiguration, commitIndex, nextIndex, matchIndex)) => {
      log.info(s"client command received ${t} from ${sender}")

      val ClientCommand(c) = t
      val m = persistence.appendLog(persistence.getCurrentTerm, Right(c), sender.path)
      log.info(s"client command appended to log")
      log.debug(s"next index updated for leader")
      stay using l.copy(nextIndex = nextIndex + (this.id -> Some(persistence.nextIndex)), matchIndex = matchIndex + (this.id -> Some(m)))
    }

    case Event(Tick, l@LeaderState(clusterConfiguration, commitIndex, nextIndexes, matchIndex)) => {
      log.info(s"sending heartbeat to followers")
      for {
        (id, path) <- peers(clusterConfiguration)
      } {
        val prevLogIndex = nextIndexes.getOrElse(id, None) match {
          case Some(i) if (i > 0) => Some(i - 1)
          case _ => None
        }
        val nextIndex = nextIndexes.getOrElse(id, None).getOrElse(0)

        println(s"follower ${id}'s nextIndex: ${nextIndex}")
        log.info(s"follower ${id}'s nextIndex: ${nextIndex}")
        println(s"sending log entries form ${nextIndex} until ${persistence.nextIndex}")
        log.info(s"sending log entries form ${nextIndex} until ${persistence.nextIndex}")
        context.actorSelection(path) ! AppendEntries(
          persistence.getCurrentTerm, this.id, prevLogIndex, prevLogIndex.flatMap(persistence.getTerm(_)),
          persistence.logsBetween(nextIndex, persistence.nextIndex), commitIndex
        )
      }
//      log.info("we have sent so much heartbeat!!!")
      stay using l
    }

    case Event(gv@GrantVote(term), l@LeaderState(clusterConfiguration, commitIndex, nextIndex, matchIndex)) => {
      if (term > persistence.getCurrentTerm) {
        log.info("Received GrantVote for a term in the future, becoming Follower")
        goto(Follower) using FollowerState(clusterConfiguration, commitIndex)
      } else {
        log.debug(s"${gv} received from ${sender}, I'm already Leader")
        stay
      }
    }

    case Event(l@LogMatchesUntil(peerId, _matchIndex), LeaderState(clusterConfiguration, commitIndex, nextIndex, matchIndex)) => {
      log.debug(s"log matches to ${_matchIndex} for peer ${peerId}")
      //TODO: verify if it's correct
//      def updateNextIndex(nextIndex: Map[Int, Option[Int]], peerId: Int, )
      val newNextIndex: Map[Int, Option[Int]] = nextIndex + (peerId -> _matchIndex.map({ i => i + 1}) )
      log.debug(s"update nextIndex from ${nextIndex} to ${newNextIndex}")
      val newMatchIndex = matchIndex + (peerId -> _matchIndex)
      log.debug(s"update matchIndex from ${matchIndex} to ${newMatchIndex}")
      val newCommitIndex: Option[Int] = RaftActor.determineCommitIndex(clusterConfiguration, newMatchIndex)
      log.debug(s"update commitIndex from ${commitIndex} to ${newCommitIndex}")
      if (commitIndex != newCommitIndex) {
        for {
          stop <- newCommitIndex
          start <- commitIndex.orElse(Some(-1))
        } yield {
          log.debug(s"leader committing log entries between ${start+1} ${stop+1}")
          persistence.logsBetween(start + 1, stop + 1).zipWithIndex.map({
            case ((Right(command), actorPath), i) =>
              stateMachine ! WrappedClientCommand(i + start + 1, Envelope(command, actorPath))
              None
            case ((Left(ReconfigureCluster(clusterConfiguration)), actorPath), i) =>
              // when we commit a reconfiguration, we should either create a new ReconfigureCluster message, either apply the final result of the reconfiguration
              clusterConfiguration match {
                case ClusterConfiguration(currentConfiguration, None) =>
                  if (!currentConfiguration.contains(id)) {
                    // if we are not anymore in the cluster
                    stop
                  }
                case ClusterConfiguration(currentConfiguration, Some(newConfiguration)) =>
                  val updatedClusterConfiguration  = ClusterConfiguration(newConfiguration, None)
                  persistence.appendLog(persistence.getCurrentTerm, Left(ReconfigureCluster(updatedClusterConfiguration)), self.path)
              }
              Some(clusterConfiguration)
          })
        }

      } else {
        log.debug("nothing to commit")
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

    case Event(Join(peerId, actorPath), l@LeaderState(clusterConfiguration, commitIndex, nextIndex, matchIndex)) => clusterConfiguration.newConfig match {
      case None =>
        val updatedClusterConfiguration = ClusterConfiguration(clusterConfiguration.currentConfig, Some(clusterConfiguration.currentConfig + (peerId -> actorPath)))
        // we send back sender, so sender get alerted when the transition is ready
        persistence.appendLog(persistence.getCurrentTerm, Left(ReconfigureCluster(updatedClusterConfiguration)), sender().path)
        stay using l.copy(clusterConfiguration = updatedClusterConfiguration)

      case Some(newConfig) =>
        stay replying AlreadyInTransition

    }


    case Event(Leave(_id), l@LeaderState(clusterConfiguration, commitIndex, nextIndex, matchIndex)) => clusterConfiguration.newConfig match {
      case None =>
        val updatedClusterConfiguration = ClusterConfiguration(clusterConfiguration.currentConfig, Some(clusterConfiguration.currentConfig - _id))
        // we send back sender, so sender get alerted when the transition is ready
        persistence.appendLog(persistence.getCurrentTerm, Left(ReconfigureCluster(updatedClusterConfiguration)), sender().path)
        stay using l.copy(clusterConfiguration = updatedClusterConfiguration)

      case Some(newConfig) =>
        stay replying AlreadyInTransition

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

  whenUnhandled {

    case Event(HeartBeat, _) => {
      println(s"member ${id} is still alive")
      stay
    }

    case Event(t@TermExpired(term), s: ClusterState) => {
      log.info(s"${t} received from ${sender()}")
      if (term > persistence.getCurrentTerm) {
        log.info(s"${t} received with bigger term than currentTerm, but staying")
//        goto(Follower) using FollowerState(s.clusterConfiguration, s.commitIndex, None)
        stay
      } else {
        log.info(s"staying leader despite of ${t}")
        stay
      }
    }

    case Event(RequestVote(term, candidateId, lastLogIndex, lastLogTerm), s: ClusterState) => {
      log.info(s"receiving vote request from ${candidateId} for term ${term}")
      if (isStale(term)) {
        log.info(s"vote request for an old term, current term is ${persistence.getCurrentTerm}")
        // NOTE: § 5.1
        // we should not send this, because we cannot be sure that our logs are committed, etc
//        stay replying TermExpired(persistence.getCurrentTerm)
        // hotfix:
        stay
      } else if ({
        // the voter denies its vote if its own log is more up-to-date than that of the candidate.
        // so votee should have a log at least as up-to-date as the candidate
//        persistence.termMatches(lastLogIndex, lastLogIndex)

        //If the logs have last entries with different terms
        if (persistence.lastLogTerm != lastLogTerm) {
          // then the log with the later term is more up-to-date
          persistence.lastLogTerm.getOrElse(-1) <= lastLogTerm.getOrElse(-1)
        } else {
          // else whichever log is longer is more up-to-date.
          persistence.lastLogIndex.getOrElse(-1) <= lastLogIndex.getOrElse(-1)
        }
      }) {

        if (isNew(term)) {
          log.info(s"vote request for a new term, updating current term and granting vote")
          persistence.setCurrentTerm(term)
          persistence.clearVotedFor()
          persistence.setVotedFor(candidateId)

          cancelHeartbeatTimer()
          setElectionTimer()

          goto(Follower) using FollowerState(s.clusterConfiguration, s.commitIndex, None) replying GrantVote(term)
        } else {
          //isCurrent
          log.info("vote request for the current term")
          persistence.getVotedFor match {
            case None =>
              persistence.setVotedFor(candidateId)
              log.info(s"vote not casted for this term, granting vote for ${candidateId}")
              stay replying GrantVote(term)
//            case Some(id) if (id == candidateId) =>
//              log.warning(s"vote already casted for ${candidateId}, not voting again")
////              log.info(s"already voted for ${candidateId}, but voting again for ${candidateId}")
////              stay replying GrantVote(term)
//              stay
            case Some(id) =>
              log.info(s"vote already casted for ${id}, not voting again")
              stay
          }
        }
      } else {
        log.info(s"election safety criteria fails, refusing to vote")
        stay
      }
    }
//    case Event(AlreadyApplied(nextIndex), s: ClusterState) => {
//
//    }

    case Event(a: AppendEntries[T], s: ClusterState) => {
      val AppendEntries(term: Int, leaderId: Int, prevLogIndex: Option[Int], prevLogTerm: Option[Int], entries: Seq[(Either[ReconfigureCluster, T], ActorPath)], leaderCommit: Option[Int]) = a

      if (isStale(term)) {
        stay replying TermExpired(persistence.getCurrentTerm)
      } else if (isCurrent(term)) {
        if (persistence.termMatches(prevLogIndex, prevLogTerm)) {
          //reset election timer
          setElectionTimer()
          persistence.appendLog(prevLogIndex, persistence.getCurrentTerm, entries)

          for {
            start <- s.commitIndex.orElse(Some(-1))
            stop <- leaderCommit
          } yield {
            // TODO: we should really do something with last applied
            persistence.logsBetween(start + 1, stop + 1).zipWithIndex.map({
              case ((Right(command), _), i) => {
                log.info("appying stuff to the statemachine in the follower")
                // no need that clients send back the results
                // TODO: actor path should be optional?
                stateMachine ! WrappedClientCommand(i + start + 1, Envelope(command, context.system.deadLetters.path))
              }
              case ((Left(ReconfigureCluster(_clusterConfiguration)), actorRef), i) => {
                log.info("committing changes in cluster configuration")
                _clusterConfiguration match {
                  case ClusterConfiguration(clusterMap, None) =>
                    if (!clusterMap.contains(id)) {
                      log.info(s"Follower ${id} not a member of the cluster anymore, stepping down")
                      stop
                    }
                  case _ =>
                }
              }
            })
          }

          val clusterConfigurationsToApply: Option[ClusterConfiguration] = entries.filter(_._1.isLeft).map(_._1.left.get.clusterConfiguration).lastOption
          clusterConfigurationsToApply foreach { l =>
            log.info(s"changing clusterConfiguration from ${s.clusterConfiguration} to ${l}!")
          }
          {
            if (stateName == Candidate)
              goto(Follower)
            else if (stateName == Follower)
              stay
            else
              stop(FSM.Failure("receiving AppendEntries for current term when we are supposed to be the Leader"))
          } using FollowerState(
            clusterConfiguration = clusterConfigurationsToApply.getOrElse(s.clusterConfiguration),
            commitIndex = {
              val m = Math.max(leaderCommit.getOrElse(-1), s.commitIndex.getOrElse(-1))
              if (m == -1) {
                None
              } else {
                Some(m)
              }
            },
            leaderId = Some(leaderId)
          ) replying LogMatchesUntil(this.id, persistence.lastLogIndex)
        } else {
          stay replying InconsistentLog(this.id)
        }

      } else if (isNew(term)) {
        persistence.setCurrentTerm(term)
        persistence.clearVotedFor()

        cancelElectionTimer()
        setElectionTimer()

        goto(Follower) using FollowerState(s.clusterConfiguration, s.commitIndex, Some(leaderId))
      } else {
        stop(Failure(s"invalid term ${term} in AppendEntries message"))
      }

    }
    case Event(e, s) =>
      log.warning("received unhandled request {} in state {}/{} from {}", e, stateName, s, sender())
      stay

  }

  /** (re)sets the timer used in Follower and Candidate states
   *
   */
  def setElectionTimer(): Unit = {
    setTimer("election", ElectionTimeout, electionTimeout * 2, true)
  }
  def cancelElectionTimer(): Unit = {
    cancelTimer("election")
  }
  def isElectionTimerActive(): Boolean = {
    isTimerActive("election")
  }


  def setLivenessTimer(): Unit = {
    setTimer("liveness", HeartBeat, 1 seconds, true)
  }

  def setHeartbeatTimer(): Unit = {
    setTimer("heartbeat", Tick, electionTimeout, true)
  }
  def cancelHeartbeatTimer(): Unit = {
    cancelTimer("heartbeat")
  }
  def isHeartbeatTimerActive(): Boolean = {
    isTimerActive("heartbeat")
  }


  onTransition {

    case Follower -> Candidate =>
      log.info("transition: Follower -> Candidate")
      log.info("starting election timer")

      setElectionTimer()

      nextStateData match {
        case CandidateState(clusterConfiguration, commitIndex, leaderId, numberOfVotes) =>
          val currentTerm = persistence.incrementAndGetTerm
          for {
            (id, path) <- clusterConfiguration.currentConfig
            if (id != this.id)
          } {
            log.info(s"requesting vote from ${id} for term ${currentTerm}")
            context.actorSelection(path) ! RequestVote(currentTerm, this.id, persistence.lastLogIndex, persistence.lastLogTerm)
          }
          log.info("voting for self")
          persistence.setVotedFor(this.id)
          self ! GrantVote(currentTerm)

        case s@LeaderState(_, _, _, _) =>
          log.error("invalid data in Candidate state: " + s)

        case s@FollowerState(_, _, _) =>
          log.error("invalid data in Candidate state: " + s)
      }

    case Candidate -> Leader => {
      cancelElectionTimer()
      setHeartbeatTimer()
      log.info("transition: Candidate -> Leader")
    }

    case Candidate -> Follower => {
      setElectionTimer()
      log.info("transition: Candidate -> Follower")
    }

    case Leader -> Follower => {
      cancelHeartbeatTimer()
      setElectionTimer()
//      cancelTimer("heartbeat")
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
            log.info(s"requesting vote from ${id} for term ${currentTerm}")
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