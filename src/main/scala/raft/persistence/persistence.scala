package raft

import akka.actor.{ActorPath, ActorRef}
import com.google.common.primitives.Bytes

import org.iq80.leveldb.DB
import raft.cluster.{ClusterConfiguration, ReconfigureCluster}
import raft.persistence.serialization.BinaryJsonSerialization.BinaryJsonSerialization
import raft.persistence.serialization.Serialization
import raft.statemachine.Command


import org.iq80.leveldb._
import org.iq80.leveldb.impl.Iq80DBFactory._
import java.io.File


import scala.util.{Try, Success, Failure}

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
    def appendLog(prevLogIndex: Option[Int], term: Int, entries: Seq[(Either[ReconfigureCluster, T], ActorPath)]): Unit

    /** Append a single log entry at the end of the log
     *
     * @param term
     * @param entry
     */
    def appendLog(term: Int, entry: Either[ReconfigureCluster, T], path: ActorPath): Int

    def getEntry(index: Int): Option[(Int, Either[ReconfigureCluster, T], ActorPath)]

    def logsBetween(start: Int, stop: Int): Seq[(Either[ReconfigureCluster, T], ActorPath)]

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

    // do we really need this?
//    def lastClusterConfigurationIndex: Option[Int]

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

  case class LevelDBPersistence[T, D](db: DB)(implicit serializer: BinaryJsonSerialization[Either[ReconfigureCluster, T]]) extends Persistence[T, D] {

    import raft.persistence.serialization._
//    import ScalaPicklingSerialization._
    import BinaryJsonSerialization._
    private def setLastIndex(index: Option[Int]) {
      db.put("lastIndex".serialize, index.serialize)
    }

    /** Append log starting from prevLogIndex
      *
      * @param prevLogIndex
      * @param term
      * @param entries
      */
    override def appendLog(prevLogIndex: Option[Int], term: Int, entries: Seq[(Either[ReconfigureCluster, T], ActorPath)]): Unit = {

      entries.zip(Stream from nextIndex) foreach {
        case ((entry, actorPath), index) =>
          setLastIndex(Some(index))
          db.put((index + "_term").serialize, term.serialize)
          db.put((index + "_value").serialize, serializer.serialize(entry))
          db.put((index + "_path").serialize, actorPath.toSerializationFormat.serialize)
      }
    }

    /** Append a single log entry at the end of the log
      *
      * @param term
      * @param entry
      */
    override def appendLog(term: Int, entry: Either[ReconfigureCluster, T], actorPath: ActorPath): Int = {
      val index = nextIndex
      setLastIndex(Some(index))
      db.put((index + "_term").serialize, term.serialize)
      db.put((index + "_value").serialize, serializer.serialize(entry))
      db.put((index + "_path").serialize, actorPath.toSerializationFormat.serialize)
      index
    }



    override def getEntry(index: Int): Option[(Int, Either[ReconfigureCluster, T], ActorPath)] = {
      for {
        term <- Option(db.get((index + "_term").serialize))
        value <- Option(db.get((index + "_value").serialize))
        actorPathString <- Option(db.get((index + "_path").serialize))
      } yield {
        (term.deserialize[Int], serializer.deserialize(value), ActorPath.fromString(actorPathString.deserialize[String]))
      }
    }

    override def snapshot: D = ???


    override def nextIndex: Int = lastLogIndex match {
      case None => 0
      case Some(i) => i + 1
    }

    override def getTerm(index: Int): Option[Int] = {
      for {
        entry <- getEntry(index)
      } yield entry._1
    }


    /** Returns None if log is empty, otherwise returns Some(t), if the term of the last log entry is t
      *
      */
    override def lastLogTerm: Option[Int] = {
      for {
        index <- lastLogIndex
        term <- getTerm(index)
      } yield term
    }

    // TODO: this should be atomic or something?
    override def incrementAndGetTerm: Int = {
      val nextTerm = getCurrentTerm + 1
      setCurrentTerm(nextTerm)
      nextTerm
    }

    override def getVotedFor: Option[Int] = {
      val serverId = db.get("votedFor".serialize)
      if (serverId == null) {
        None
      } else {
        Some(serverId.deserialize[Int])
      }
    }

    override def setVotedFor(serverId: Int): Unit = {
      db.put(bytes("votedFor"), serverId.serialize)
    }

    override def clearVotedFor(): Unit = {
      db.delete(bytes("votedFor"))
    }

//    override def lastClusterConfigurationIndex: Option[Int] = ???

    /** Returns None if log is empty, otherwise returns Some(l-1), if log is of length l
      *
      */
    override def lastLogIndex: Option[Int] = {
      val key = "lastIndex".serialize
      val lastIndex = db.get(key)
      if (lastIndex == null) {
        None
      } else {
        lastIndex.deserialize[Option[Int]]
      }
    }

    override def getCurrentTerm: Int = {
      val currentTerm = db.get("currentTerm".serialize)
      if (currentTerm == null) {
        0
      } else {
        currentTerm.deserialize[Int]
      }
    }

    override def setCurrentTerm(term: Int): Unit = {
      db.put("currentTerm".serialize, term.serialize)
    }

    override def termMatches(prevLogIndex: Option[Int], prevLogTerm: Option[Int]): Boolean = prevLogIndex match {
      case None =>
        true
      case Some(index) =>
        prevLogTerm == getTerm(index)
    }

    override def logsBetween(start: Int, stop: Int): Seq[(Either[ReconfigureCluster, T], ActorPath)] = for {
      i <- start until stop
      entry <- getEntry(i)
    } yield (entry._2, entry._3)
  }

  case class InMemoryPersistence() extends Persistence[Command, Map[String, Int]] {
    var logs = Vector[(Int, Either[ReconfigureCluster, Command], ActorPath)]()
    var currentTerm: Int = 0
    var votedFor: Option[Int] = None

    override def appendLog(prevLogIndex: Option[Int], term: Int, entries: Seq[(Either[ReconfigureCluster, Command], ActorPath)]): Unit = prevLogIndex match {
      case None => logs = entries.map(i => (term, i._1, i._2)).toVector
      case Some(index) => logs = logs.take(index + 1) ++ entries.map(i => (term, i._1, i._2))
    }

    /** Append a single log entry at the end of the log
      *
      * @param term
      * @param entry
      */
    override def appendLog(term: Int, entry: Either[ReconfigureCluster, Command], path: ActorPath): Int = {
      logs = logs :+ ((term, entry, path))
      logs.length
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

//    override def lastClusterConfigurationIndex: Option[Int] = ???

    override def logsBetween(start: Int, stop: Int): Seq[(Either[ReconfigureCluster, Command], ActorPath)] = logs.slice(start, stop).map( i => (i._2, i._3) )

    override def nextIndex: Int = logs.size

    override def getEntry(index: Int): Option[(Int, Either[ReconfigureCluster, Command], ActorPath)] = logs.lift(index)
  }

}
