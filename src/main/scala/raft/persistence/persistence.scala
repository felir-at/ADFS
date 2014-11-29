package raft

import akka.actor.ActorRef
import raft.cluster.ReconfigureCluster
import raft.statemachine.Command

/**
 * Created by kosii on 2014. 10. 26..
 */
package object persistence {

  trait Persistence[T, D] {

    /** Append log starting from prevLogIndex
     *
     * @param prevLogIndex
     * @param term
     * @param entries
     */
    def appendLog(prevLogIndex: Option[Int], term: Int, entries: Seq[(Either[ReconfigureCluster, T], ActorRef)]): Unit

    /** Append a single log entry at the end of the log
     *
     * @param term
     * @param entry
     */
    def appendLog(term: Int, entry: Either[ReconfigureCluster, T], ref: ActorRef): Unit

    def logsBetween(start: Int, stop: Int): Seq[(Either[ReconfigureCluster, T], ActorRef)]

    /** Returns None if log is empty, otherwise returns Some(l-1), if log is of length l
      *
      */
    // TODO: rename it to lastIndex to be more consistent with the paper
    def lastLogIndex: Option[Int]

    def nextIndex: Int

    /** Returns None if log is empty, otherwise returns Some(t), if the term of the last log entry is t
      *
      */
    def lastLogTerm: Option[Int]

    def lastClusterConfigurationIndex: Option[Int]

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

  case class InMemoryPersistence() extends Persistence[Command, Map[String, Int]] {
    var logs = Vector[(Int, Either[ReconfigureCluster, Command], ActorRef)]()
    var currentTerm: Int = 0
    var votedFor: Option[Int] = None

    override def appendLog(prevLogIndex: Option[Int], term: Int, entries: Seq[(Either[ReconfigureCluster, Command], ActorRef)]): Unit = prevLogIndex match {
      case None => logs = entries.map(i => (term, i._1, i._2)).toVector
      case Some(index) => logs = logs.take(index + 1) ++ entries.map(i => (term, i._1, i._2))
    }

    /** Append a single log entry at the end of the log
      *
      * @param term
      * @param entry
      */
    override def appendLog(term: Int, entry: Either[ReconfigureCluster, Command], ref: ActorRef): Unit = {
      logs = logs :+ ((term, entry, ref))
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
      case None =>
        true
      case Some(index) =>
        prevLogTerm == getTerm(index)
    }

    override def lastClusterConfigurationIndex: Option[Int] = ???

    override def logsBetween(start: Int, stop: Int): Seq[(Either[ReconfigureCluster, Command], ActorRef)] = logs.slice(start, stop).map( i => (i._2, i._3) )

    override def nextIndex: Int = logs.size

  }

}