using Akka.Actor;
using Akka.Persistence.TCK.Journal;
using Akka.Persistence.TCK.Serialization;
using Xunit;

namespace Akka.Persistence.DynamoDb.Test.Journal
{
    [Collection(DynamoDbTestCollection.Name)]
    public class DynamoDbJournalSpec : JournalSpec
    {
        public DynamoDbJournalSpec(LocalstackDynamoDbFixture fixture)
            : base(DynamoDbStorageConfigHelper.DynamoDbConfig(fixture))
        {
            DynamoDbPersistence.Get(Sys);
            Initialize();
        }

        protected override bool SupportsSerialization { get; } = false;

        protected override bool SupportsRejectingNonSerializableObjects { get; } = false;

        [Fact]
        public void Stuff()
        {
            //var senderProbe = CreateTestProbe();
            var receiverProbe = CreateTestProbe();

            //Journal_should_serialize_events

            if (!SupportsSerialization) return;

            var probe = CreateTestProbe();
            var payload = new TestPayload(probe.Ref);

            //ShardRegion.StartEntity

            var atomicWrite = new AtomicWrite(new Persistent(payload, 6L, Pid, sender: ActorRefs.NoSender, writerGuid: WriterGuid));
            var writeMessages = new WriteMessages(new[] { atomicWrite }, probe.Ref, ActorInstanceId);

            Journal.Tell(writeMessages);

            probe.ExpectMsg<WriteMessagesSuccessful>();
            var pid = Pid;
            var writerGuid = WriterGuid;
            probe.ExpectMsg<WriteMessageSuccess>(o =>
            {
                Assertions.AssertEqual(writerGuid, o.Persistent.WriterGuid);
                Assertions.AssertEqual(pid, o.Persistent.PersistenceId);
                Assertions.AssertEqual(6L, o.Persistent.SequenceNr);
                Assertions.AssertTrue(o.Persistent.Sender == ActorRefs.NoSender || o.Persistent.Sender.Equals(Sys.DeadLetters), $"Expected WriteMessagesSuccess.Persistent.Sender to be null or {Sys.DeadLetters}, but found {o.Persistent.Sender}");
                Assertions.AssertEqual(payload, o.Persistent.Payload);
            });

            Journal.Tell(new ReplayMessages(6L, long.MaxValue, long.MaxValue, Pid, receiverProbe.Ref));

            receiverProbe.ExpectMsg<ReplayedMessage>(o =>
            {
                Assertions.AssertEqual(writerGuid, o.Persistent.WriterGuid);
                Assertions.AssertEqual(pid, o.Persistent.PersistenceId);
                Assertions.AssertEqual(6L, o.Persistent.SequenceNr);
                Assertions.AssertTrue(o.Persistent.Sender == ActorRefs.NoSender || o.Persistent.Sender.Equals(Sys.DeadLetters), $"Expected WriteMessagesSuccess.Persistent.Sender to be null or {Sys.DeadLetters}, but found {o.Persistent.Sender}");
                Assertions.AssertEqual(payload, o.Persistent.Payload);
            });

            Assertions.AssertEqual(receiverProbe.ExpectMsg<RecoverySuccess>().HighestSequenceNr, 6L);
        }
    }
}