package raft

import akka.actor.{ActorRef, ActorPath}

import scala.language.{higherKinds, postfixOps}
import scala.util.Try

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
  case class AppendEntries[T](term: Int, leaderId: Int, prevLogIndex: Option[Int], prevLogTerm: Option[Int], entries: Seq[(T, ActorRef)], leaderCommit: Option[Int]) extends RPC
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




}

