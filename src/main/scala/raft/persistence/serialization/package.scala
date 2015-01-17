package raft.persistence

import akka.actor.ActorPath
import raft.cluster.{ClusterConfiguration, ReconfigureCluster}
import raft.statemachine.Command
import raft.statemachine.Command.CommandPickling

import scala.util.{Try, Success, Failure}

/**
 * Created by kosii on 15. 01. 13..
 */
package object serialization {

  trait Serialization[T, D] {
    def serialize(t: T): D
    def deserialize(d: D): T
  }

  implicit class Serializer[T, D](t: T)(implicit v: Serialization[T, D]) {
    def serialize: D = v.serialize(t)
  }

  implicit class Deserializer[D](d: D) {
    def deserialize[T](implicit v: Serialization[T, D]): T = v.deserialize(d)
  }

  object ScalaPicklingSerialization {
    sealed trait ScalaPicklingSerialization[T] extends Serialization[T, Array[Byte]]
    import scala.pickling._
    import binary._

    implicit object IntScalaPicklingSerialization extends ScalaPicklingSerialization[Int] {
      override def serialize(t: Int): Array[Byte] = t.pickle.value
      override def deserialize(d: Array[Byte]): Int = BinaryPickleArray(d).unpickle[Int]
    }

    implicit object StringScalaPicklingSerialization extends ScalaPicklingSerialization[String] {
      override def serialize(t: String): Array[Byte] = t.pickle.value
      override def deserialize(d: Array[Byte]): String = BinaryPickleArray(d).unpickle[String]
    }

    implicit object OptIntScalaPicklingSerialization extends ScalaPicklingSerialization[Option[Int]] {
      override def serialize(t: Option[Int]): Array[Byte] = t.pickle.value
      override def deserialize(d: Array[Byte]): Option[Int] = BinaryPickleArray(d).unpickle[Option[Int]]
    }

    implicit object CommandScalaPicklingSerialization extends ScalaPicklingSerialization[Either[ReconfigureCluster, Command]] {

      implicit val commandPickling = new CommandPickling()
      private def mapToSerializationFormat(clusterConfigurationMap: Map[Int, ActorPath]) = for {
        (k, v) <- clusterConfigurationMap
      } yield (k, v.toSerializationFormat)

      private def mapFromSerializationFormat(serializedClusterConfigurationMap: Map[Int, String]) = for {
        (k, v) <- serializedClusterConfigurationMap
      } yield (k, ActorPath.fromString(v))

      private def toSerializationFormat(command: ReconfigureCluster) = command match {
        case ReconfigureCluster(ClusterConfiguration(currentConfig, newConfig)) =>
          (mapToSerializationFormat(currentConfig), newConfig map mapToSerializationFormat)
      }

      private def fromSerializationFormat(r: (Map[Int, String], Option[Map[Int, String]])) = r match {
        case (c, optC) => ReconfigureCluster(ClusterConfiguration(mapFromSerializationFormat(c), optC map mapFromSerializationFormat))
      }

      override def serialize(t: Either[ReconfigureCluster, Command]): Array[Byte] = t match {
        case Left(reconfigureCluster) => toSerializationFormat(reconfigureCluster).pickle.value
        case Right(clientCommand) => clientCommand.pickle.value
      }

      override def deserialize(value: Array[Byte]): Either[ReconfigureCluster, Command] = {
        Try {
          BinaryPickleArray(value).unpickle[(Map[Int, String], Option[Map[Int, String]])]
        } match {
          case Failure(err) => Right(BinaryPickleArray(value).unpickle[Command])
          case Success(v) => Left(fromSerializationFormat(v))
        }
      }
    }

  }

}
