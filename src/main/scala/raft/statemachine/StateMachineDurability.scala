package raft.statemachine

import java.io._

import org.iq80.leveldb.DB
import org.iq80.leveldb.impl.Iq80DBFactory._

import scala.math._

/**
 * Created by kosii on 14. 12. 07..
 */
sealed trait StateMachineDurability {
  def getLastApplied: Int
  def setLastApplied(index: Int)
}

case class InMemoryStateMachine() extends StateMachineDurability {
  var index = -1
  override def getLastApplied: Int = index

  override def setLastApplied(index: Int): Unit = this.index = max(index, getLastApplied)
}

case class OnDiskStateMachine(file: File) extends StateMachineDurability {

  override def getLastApplied: Int = try {
    new DataInputStream(new BufferedInputStream(new FileInputStream(file))).readInt()
  } catch {
    case fnfe: FileNotFoundException => -1
    case e: Exception => throw e
  }

  override def setLastApplied(index: Int): Unit = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file))).writeInt(max(index, getLastApplied))

}

case class LevelDBStateMachine(db: DB) extends StateMachineDurability {
  override def getLastApplied: Int = {
    //      val options: Options = new Options()
    //      options.createIfMissing(true)
    //      val db: DB = factory.open(new File(dbnsame), options)

    val res = db.get(bytes("lastApplied"))
    if (res == null) {
      -1
    } else {
      asString(res).toInt
    }
  }

  override def setLastApplied(index: Int): Unit = {

    //      val options: Options = new Options()
    //      options.createIfMissing(true)
    //      val db: DB = factory.open(new File(dbname), options)

    val lastApplied = getLastApplied
    db.put(bytes("lastApplied"), bytes(max(index, lastApplied).toString))
  }
}
