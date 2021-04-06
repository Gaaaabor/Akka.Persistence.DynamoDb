using Akka.Actor;
using Akka.Event;
using Akka.Persistence.DynamoDb.Query.QueryApi;
using Akka.Persistence.Query;
using Akka.Streams.Actors;

namespace Akka.Persistence.DynamoDb.Query.Publishers
{
    internal abstract class AbstractEventsByTagPublisher : ActorPublisher<EventEnvelope>
    {
        private ILoggingAdapter? _log;

        protected readonly DeliveryBuffer<EventEnvelope> Buffer;
        protected readonly IActorRef JournalRef;
        protected long CurrentOffset;
        protected AbstractEventsByTagPublisher(string tag, long fromOffset, int maxBufferSize, string writeJournalPluginId)
        {
            Tag = tag;
            CurrentOffset = FromOffset = fromOffset;
            MaxBufferSize = maxBufferSize;
            Buffer = new DeliveryBuffer<EventEnvelope>(OnNext);
            JournalRef = Persistence.Instance.Apply(Context.System).JournalFor(writeJournalPluginId);
        }

        protected ILoggingAdapter Log => _log ??= Context.GetLogger();
        protected string Tag { get; }
        protected long FromOffset { get; }
        protected abstract long ToOffset { get; }
        protected int MaxBufferSize { get; }

        protected bool IsTimeForReplay => (Buffer.IsEmpty || Buffer.Length <= MaxBufferSize / 2) && (CurrentOffset <= ToOffset);

        protected abstract void ReceiveInitialRequest();
        protected abstract void ReceiveIdleRequest();
        protected abstract void ReceiveRecoverySuccess(long highestSequenceNr);

        protected override bool Receive(object message) => message.Match()
            .With<Request>(_ => ReceiveInitialRequest())
            .With<EventsByTagPublisher.Continue>(() => { })
            .With<Cancel>(_ => Context.Stop(Self))
            .WasHandled;

        protected bool Idle(object message) => message.Match()
            .With<EventsByTagPublisher.Continue>(() => {
                if (IsTimeForReplay) Replay();
            })
            .With<TaggedEventAppended>(() => {
                if (IsTimeForReplay) Replay();
            })
            .With<Request>(ReceiveIdleRequest)
            .With<Cancel>(() => Context.Stop(Self))
            .WasHandled;

        protected void Replay()
        {
            var limit = MaxBufferSize - Buffer.Length;
            Log.Debug("request replay for tag [{0}] from [{1}] to [{2}] limit [{3}]", Tag, CurrentOffset, ToOffset, limit);
            JournalRef.Tell(new ReplayTaggedMessages(CurrentOffset, ToOffset, limit, Tag, Self));
            Context.Become(Replaying());
        }

        protected Receive Replaying()
        {
            return message => message.Match()
                .With<ReplayedTaggedMessage>(replayed => {
                    Buffer.Add(new EventEnvelope(
                        offset: new Sequence(replayed.Offset),
                        persistenceId: replayed.Persistent.PersistenceId,
                        sequenceNr: replayed.Persistent.SequenceNr,
                        timestamp: replayed.Persistent.Timestamp,
                        @event: replayed.Persistent.Payload));

                    CurrentOffset = replayed.Offset;
                    Buffer.DeliverBuffer(TotalDemand);
                })
                .With<RecoverySuccess>(success => {
                    Log.Debug("replay completed for tag [{0}], currOffset [{1}]", Tag, CurrentOffset);
                    ReceiveRecoverySuccess(success.HighestSequenceNr);
                })
                .With<ReplayMessagesFailure>(failure => {
                    Log.Debug("replay failed for tag [{0}], due to [{1}]", Tag, failure.Cause.Message);
                    Buffer.DeliverBuffer(TotalDemand);
                    OnErrorThenStop(failure.Cause);
                })
                .With<Request>(_ => Buffer.DeliverBuffer(TotalDemand))
                .With<EventsByTagPublisher.Continue>(() => { })
                .With<TaggedEventAppended>(() => { })
                .With<Cancel>(() => Context.Stop(Self))
                .WasHandled;
        }
    }
}