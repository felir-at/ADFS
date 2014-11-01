package cluster

import java.io.File

import adfs.utils._
import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory

/**
 * Created by kosii on 2014.10.02..
 */
case class FileStorage(systemName: String, host: String, port: Int) {

  val system = ActorSystem("system", remoteConfig(host, port, ConfigFactory.load()))
  val fileRegistry = system.actorOf(Props[FileRegistry], "fileRegistry")
  val fileSystem = system.actorOf(FileSystemActor.props(new File("/Users/kosii/Projects/ADFS/dataDir/system"+port), fileRegistry), "fileSystem")
//  system1.actorOf(MemberActor.props(new File("/tmp/system1")), "node")


}
