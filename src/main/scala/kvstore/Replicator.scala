package kvstore

import akka.actor.{Actor, ActorRef, Props, Terminated}
import scala.concurrent.duration._
import scala.collection.mutable

object Replicator {

  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)

  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {

  import Replicator._

  import context.dispatcher

  private case object SendPendingSnapshots

  private case class Acknowledgement(target: ActorRef, id: Long)
  private case class PendingSnapshot(id: Long, msg: Any)

  // logical clock
  private var lClock = 0L
  // acknowledgements
  private val acks = mutable.HashMap.empty[Long, List[Acknowledgement]]
  // not-yet-confirmed snapshots
  private val pendingSnapshots = mutable.HashMap.empty[String, PendingSnapshot]

  private def getLClockAndIncrement = {
    val ret = lClock
    lClock += 1
    ret
  }

  def receive: Receive = {
    case Replicate(key, vOpt, id) => {
      val seq = getLClockAndIncrement
      val msg = Snapshot(key, vOpt, seq)
      val newAcks = Acknowledgement(sender, id) ::
        pendingSnapshots.put(key, PendingSnapshot(seq, msg))
        .flatMap { s => acks.get(s.id) }
        .getOrElse { List.empty[Acknowledgement] }
      acks.put(seq, newAcks)
    }

    case SnapshotAck(key, seq) => {
      if (pendingSnapshots.get(key).exists { seq == _.id }) {
        pendingSnapshots.remove(key)
        .flatMap { s => acks.remove(s.id) }
        .getOrElse { List.empty[Acknowledgement] }
        .reverse
        .foreach { a => a.target ! Replicated(key, a.id) }
      }
    }

    case SendPendingSnapshots => {
      pendingSnapshots.values.toIndexedSeq.sortBy { _.id }
      .foreach { replica ! _.msg }
    }

    case Terminated(`replica`) => {
      context.stop(self)
    }
  }

  // We die together!
  context.watch(replica)
  // We retry snapshots every 100 ms
  context.system.scheduler.schedule(0 millis, 100 millis, self, SendPendingSnapshots)

}
