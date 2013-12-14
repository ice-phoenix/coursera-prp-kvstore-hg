package kvstore

import akka.actor.{Actor, ActorRef, OneForOneStrategy, Props, Terminated}
import scala.collection.mutable
import scala.concurrent.duration._

import kvstore.Arbiter._

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

  import akka.actor.SupervisorStrategy._
  import Persistence._
  import Replica._
  import Replicator._

  import context.dispatcher

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 2, withinTimeRange = 1 second) {
      case _: Exception => Stop
    }

  private case class TimedOut(id: Long)
  private case object SendPendingPersists

  private def createPersistence = {
    val res = context.actorOf(persistenceProps)
    context.watch(res)
    res
  }

  // KV store
  private var kv = Map.empty[String, String]
  // logical clock
  private var lClock = 0L
  // persistence storage
  private var persistence = createPersistence
  // a map from secondary replicas to replicators
  private val secondaries = mutable.HashMap.empty[ActorRef, ActorRef]



  private case class Acknowledgement(target: ActorRef, success: Option[Any], failure: Option[Any])
  private case class PendingPersist(msg: Any)
  private case class PendingReplicate(msg: Any, waitingOn: Set[ActorRef]) {
    def isEmpty = waitingOn.isEmpty
    def completeOn(a: ActorRef) = PendingReplicate(msg, waitingOn - a)
  }

  // acknowledgements
  private val acks = mutable.HashMap.empty[Long, Acknowledgement]
  // not-yet-confirmed persistence msgs
  private val pendingPersists = mutable.HashMap.empty[Long, PendingPersist]
  // not-yet-confirmed replications
  private val pendingReplicates = mutable.HashMap.empty[Long, PendingReplicate]



  def receive = {
    case JoinedPrimary => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }



  private def addAcknowledgement(id: Long, a: Acknowledgement) = acks.put(id, a)
  private def addPendingPersist(id: Long, persistMsg: Any) = {
    pendingPersists.put(id, PendingPersist(persistMsg))
    persistence ! persistMsg
  }
  private def addPendingReplicate(id: Long, replicateMsg: Any) = {
    val replicators = secondaries.values.toSet
    if ( ! replicators.isEmpty ) {
      pendingReplicates.put(id, PendingReplicate(replicateMsg, replicators))
      replicators.foreach { _ ! replicateMsg }
    }
  }

  private def removePendingPersist(id: Long) = pendingPersists.remove(id)
  private def removePendingReplicate(id: Long)(implicit sender: ActorRef) = {
    pendingReplicates.get(id)
    .map { _.completeOn(sender) }
    .foreach { pr => if (pr.isEmpty) pendingReplicates.remove(id) else pendingReplicates.put(id, pr) }
  }

  private def tryComplete(id: Long) = {
    (pendingPersists.get(id), pendingReplicates.get(id)) match {
      case (None, None) => acks.remove(id).map { a => a.success.map { a.target ! _ } }
      case _ => {}
    }
  }
  private def complete(id: Long) = {
    acks.remove(id).map { a => a.success.map { a.target ! _ } }
    pendingPersists.remove(id)
    pendingReplicates.remove(id)
  }
  private def fail(id: Long) = {
    acks.remove(id).map { a => a.failure.map { a.target ! _ } }
    pendingPersists.remove(id)
    pendingReplicates.remove(id)
  }

  private def enableTimeout(id: Long) = {
    context.system.scheduler.scheduleOnce(1 second, self, TimedOut(id))
  }



  private def addSecondary(replica: ActorRef) = secondaries.put(replica, context.actorOf(Replicator.props(replica)))
  private def removeSecondary(replica: ActorRef, replicator: ActorRef) = {
    secondaries.remove(replica)
    context.stop(replicator)
    pendingReplicates
    .filter { case (_, pr) => pr.waitingOn.contains(replicator) }
    .foreach { case (id, _) => complete(id) }
  }



  val processGet: Receive = {
    case Get(k, id) => {
      sender ! GetResult(k, kv.get(k), id)
    }
  }

  val processUpdates: Receive = {
    case Insert(k, v, id) => {
      kv = kv + ((k, v))
      addAcknowledgement(id, Acknowledgement(sender, Some(OperationAck(id)), Some(OperationFailed(id))))
      addPendingPersist(id, Persist(k, Some(v), id))
      addPendingReplicate(id, Replicate(k, Some(v), id))
      enableTimeout(id)
    }
    case Remove(k, id) => {
      kv = kv - k
      addAcknowledgement(id, Acknowledgement(sender, Some(OperationAck(id)), Some(OperationFailed(id))))
      addPendingPersist(id, Persist(k, None, id))
      addPendingReplicate(id, Replicate(k, None, id))
      enableTimeout(id)
    }
  }

  val processSnapshots: Receive = {
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
      addAcknowledgement(seq, Acknowledgement(sender, Some(SnapshotAck(k, seq)), None))
      addPendingPersist(seq, Persist(k, vOpt, seq))
      enableTimeout(seq)
      lClock += 1
    }
  }

  val processTimeout: Receive = {
    case TimedOut(id) => {
      fail(id)
    }
  }

  val processPersistence: Receive = {
    case Persisted(k, id) => {
      removePendingPersist(id)
      tryComplete(id)
    }
    case SendPendingPersists => {
      pendingPersists.values.foreach { pr => persistence ! pr.msg }
    }
    case Terminated(child) if persistence == child => {
      persistence = createPersistence
      self ! SendPendingPersists
    }
  }

  val processReplication: Receive = {
    case Replicas(replicas) => {
      secondaries
      .filterNot { case (r, _) => replicas.contains(r) }
      .foreach { case (r, rr) => removeSecondary(r, rr) }

      replicas
      .filterNot { _ == self }
      .filterNot { secondaries.contains(_) }
      .foreach { addSecondary(_) }
    }
    case Replicated(k, id) => {
      removePendingReplicate(id)
      tryComplete(id)
    }
  }



  val leader: Receive = processGet orElse processUpdates orElse processTimeout orElse processPersistence orElse processReplication
  val replica: Receive = processGet orElse processSnapshots orElse processTimeout orElse processPersistence

  // We're ready!
  arbiter ! Join
  // We retry persistence every 100 ms
  context.system.scheduler.schedule(0 millis, 100 millis, self, SendPendingPersists)

}
