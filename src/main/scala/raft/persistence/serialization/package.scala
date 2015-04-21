package raft.persistence

import akka.actor.ActorPath
import raft.cluster.{ClusterConfiguration, ReconfigureCluster}
import raft.statemachine.{DeleteValue, SetValue, GetValue, Command}
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

  object BinaryJsonSerialization {

    import play.api.libs.json._
    import play.api.libs.functional._

    sealed trait BinaryJsonSerialization[T] extends Serialization[T, Array[Byte]]

    implicit object IntSerialization extends BinaryJsonSerialization[Int] {
      override def serialize(t: Int): Array[Byte] = Json.toJson(t).toString().getBytes()
      override def deserialize(d: Array[Byte]): Int = Json.fromJson[Int](Json.parse(d)).get
    }

    implicit object StringSerialization extends BinaryJsonSerialization[String] {
      override def serialize(t: String): Array[Byte] = Json.toJson(t).toString.getBytes
      override def deserialize(d: Array[Byte]): String = Json.fromJson[String](Json.parse(d)).get
    }

    implicit object OptIntSerialization extends BinaryJsonSerialization[Option[Int]] {
      override def serialize(t: Option[Int]): Array[Byte] = Json.toJson(t).toString.getBytes
      override def deserialize(d: Array[Byte]): Option[Int] = Json.fromJson[Option[Int]](Json.parse(d)).get
    }

    implicit object CommandSerialization extends BinaryJsonSerialization[Either[ReconfigureCluster, Command]] {
      implicit val commandFormat = new Format[Command] {
        override def reads(json: JsValue): JsResult[Command] = {
//          for {
//            tpe <- (json \ "tpe").validate[String]
//            key <- (json \ "key").validate[String]
//          } yield key match {
//            case "get" => JsSuccess(GetValue(key))
//            case "set" => for {
//              value <- (json \ "value").validate[Int]
//            } yield SetValue(key, value)
//            case "delete" => JsSuccess(DeleteValue(key))
//          }

          (json \ "tpe").validate[String] flatMap { tpe =>
            (json \ "key").validate[String] flatMap { key =>
              tpe match {
                case "get" => JsSuccess(GetValue(key))
                case "set" =>
                  (json \ "value").validate[Int] map { value =>
                    SetValue(key, value)
                  }
                case "delete" => JsSuccess(DeleteValue(key))
              }

            }
          }

        }
        override def writes(o: Command): JsValue = o match {
          case GetValue(key) => JsObject(Seq("tpe"->JsString("get"), "key"->JsString(key)))
          case SetValue(key, value) => JsObject(Seq("tpe"->JsString("set"), "key"->JsString(key), "value"->JsNumber(value)))
          case DeleteValue(key) => JsObject(Seq("tpe"->JsString("delete"), "key"->JsString(key)))
        }
      }
      case class Serialized(currentConfig: Map[Int, String], newConfig: Option[Map[Int, String]])

      private def mapToSerializationFormat(clusterConfigurationMap: Map[Int, ActorPath]) = for {
        (k, v) <- clusterConfigurationMap
      } yield (k, v.toSerializationFormat)

      private def mapFromSerializationFormat(serializedClusterConfigurationMap: Map[Int, String]) = for {
        (k, v) <- serializedClusterConfigurationMap
      } yield (k, ActorPath.fromString(v))

      private def toSerializationFormat(command: ReconfigureCluster): Serialized = command match {
        case ReconfigureCluster(ClusterConfiguration(currentConfig, newConfig)) =>
          Serialized(mapToSerializationFormat(currentConfig), newConfig map mapToSerializationFormat)
      }

      private def fromSerializationFormat(r: Serialized) = r match {
        case Serialized(c, optC) => ReconfigureCluster(ClusterConfiguration(mapFromSerializationFormat(c), optC map mapFromSerializationFormat))
      }
      implicit val mapFormat = new Format[Map[Int, String]] {
        override def reads(json: JsValue): JsResult[Map[Int, String]] = Json.fromJson[Map[String, String]](json) map {
          for {
            (k, v) <- _
          } yield (k.toInt, v)
        }

        override def writes(o: Map[Int, String]): JsValue = Json.toJson(
          for {
            (k, v) <- o
          } yield (k.toString, v)
        )
      }
      implicit val serializedFormat = Json.format[Serialized]
      implicit val reconfigureClusterFormat = new Format[ReconfigureCluster] {
        override def reads(json: JsValue): JsResult[ReconfigureCluster] = Json.fromJson[Serialized](json) map fromSerializationFormat
        override def writes(o: ReconfigureCluster): JsValue = Json.toJson(toSerializationFormat(o))
      }

      implicit val f = new Format[Either[ReconfigureCluster, Command]] {
        override def reads(json: JsValue): JsResult[Either[ReconfigureCluster, Command]] = {
          (json \ "tpe").validate[String] flatMap {
            case "left" =>
              (json \ "content").validate[ReconfigureCluster] map Left.apply
            case "right" =>
              (json \ "content").validate[Command] map Right.apply
          }
        }
        override def writes(o: Either[ReconfigureCluster, Command]): JsValue = o match {
          case Left(reconfigureCluster) => JsObject(Seq("tpe"->JsString("left"), "content"->Json.toJson(reconfigureCluster)))
          case Right(command) => JsObject(Seq("tpe"->JsString("right"), "content"->Json.toJson(command)))

        }
      }

      override def serialize(t: Either[ReconfigureCluster, Command]): Array[Byte] = Json.toJson(t).toString.getBytes
      override def deserialize(d: Array[Byte]): Either[ReconfigureCluster, Command] = Json.fromJson[Either[ReconfigureCluster, Command]](Json.parse(d)).get
    }
  }


  @deprecated
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
