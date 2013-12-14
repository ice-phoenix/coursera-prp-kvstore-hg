package kvstore

import akka.actor.{OneForOneStrategy, Props, ActorRef, Actor}
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ask, pipe}
import akka.actor.Terminated
import scala.concurrent.duration._
import akka.actor.PoisonPill
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy
import akka.util.Timeout
import scala.collection.mutable

object Replica {

  sealed trait Operation {
    def key: String
    def id: Long
  }

  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {

  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher
  import akka.actor.SupervisorStrategy._

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 2, withinTimeRange = 1 second) {
      case _: Exception => Stop
    }

  private case class TimedOut(val id: Long)

  // KV store
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]
  // logical clock
  var lClock = 0L
  // persistence storage
  var persistence = context.actorOf(persistenceProps)
  // not-yet-confirmed persistence msgs
  var pending = mutable.HashMap.empty[Long, (ActorRef, Persist)]

  def receive = {
    case JoinedPrimary => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  val leader: Receive = {
    case Insert(k, v, id) => {
      kv = kv + ((k, v))
      sender ! OperationAck(id)
    }
    case Remove(k, id) => {
      kv = kv - k
      sender ! OperationAck(id)
    }
    case Get(k, id) => {
      sender ! GetResult(k, kv.get(k), id)
    }
  }

  def forwardToPersistence(from: ActorRef, key: String, valueOption: Option[String], id: Long) = {
    val persist = Persist(key, valueOption, id)
    persistence ! persist
    pending.put(id, (from, persist))

    context.system.scheduler.scheduleOnce(1 second, self, TimedOut(id))
  }

  val replica: Receive = {
    case Get(k, id) => {
      sender ! GetResult(k, kv.get(k), id)
    }

    case Snapshot(k, vOpt, seq) if seq > lClock => {
      // ignore
    }
    case Snapshot(k, vOpt, seq) if seq < lClock => {
      sender ! SnapshotAck(k, seq)
    }
    case Snapshot(k, vOpt, seq) /* if seq == lClock */ => {
      vOpt match {
        case Some(v) => kv = kv + ((k, v))
        case None => kv = kv - k
      }
      forwardToPersistence(sender, k, vOpt, seq)
      lClock += 1
    }

    case Persisted(k, id) => {
      pending.remove(id)
      .map { case (target, _) => target ! SnapshotAck(k, id) }
    }
    case TimedOut(id) => {
      pending.remove(id)
    }

    case Terminated(child) if persistence == child => {
      persistence = context.actorOf(persistenceProps)
      pending.values.foreach { case (_, msg) => persistence ! msg }
    }
  }

  // We're watching you!
  context.watch(persistence)
  // We're ready!
  arbiter ! Join

}
