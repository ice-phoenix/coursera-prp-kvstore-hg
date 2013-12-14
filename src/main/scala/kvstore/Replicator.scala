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
  import Replica._
  import context.dispatcher

  private case object SendPendingSnapshots

  // map from sequence number to (sender, request id)
  var acks = mutable.HashMap.empty[Long, List[(ActorRef, Long)]]
  // not-yet-sent snapshots
  var pending = mutable.HashMap.empty[String, Snapshot]
  // logical clock
  var lClock = 0L

  def getLClockAndIncrement = {
    val ret = lClock
    lClock += 1
    ret
  }

  def receive: Receive = {
    case Replicate(key, vOpt, id) => {
      val seq = getLClockAndIncrement
      val oldAcks = pending.put(key, Snapshot(key, vOpt, seq)) match {
        case None => List.empty[(ActorRef, Long)]
        case Some(s) => acks.getOrElse(s.seq, List.empty[(ActorRef, Long)])
      }
      val newAcks = (sender, id) :: oldAcks
      acks.put(seq, newAcks)
    }
    case SnapshotAck(key, seq) => {
      if (seq == pending.get(key).seq) {
        pending.remove(key)
        .flatMap { s => acks.remove(s.seq) }
        .map { l => l.reverse }
        .getOrElse { List.empty[(ActorRef, Long)] }
        .foreach {
          case (target, requestId) => target ! Replicated(key, requestId)
        }
      }
    }
    case SendPendingSnapshots => {
      pending.values.toIndexedSeq.sortBy { _.seq }
      .foreach { replica ! _ }
    }
    case Terminated(`replica`) => {
      context.stop(self)
    }
  }

  // We die together!
  context.watch(replica)
  // We retry once every 100 ms
  context.system.scheduler.schedule(0 millis, 100 millis, self, SendPendingSnapshots)

}
