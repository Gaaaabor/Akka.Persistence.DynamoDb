using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using Akka.Event;
using Akka.Persistence.DynamoDb.Extensions;
using Akka.Persistence.Snapshot;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;

namespace Akka.Persistence.DynamoDb.Snapshot
{
    public class DynamoDbSnapshotStore : SnapshotStore, IWithUnboundedStash
    {
        private readonly ActorSystem _actorSystem;
        private readonly AmazonDynamoDBClient _client;
        private readonly DynamoDbSnapshotStoreSettings _settings;
        private readonly ILoggingAdapter _log = Context.GetLogger();

        private Table? _table;

        public DynamoDbSnapshotStore(Config? config = null)
        {
            _actorSystem = Context.System;

            _settings = config is null
                ? DynamoDbPersistence.Get(Context.System).SnapshotSettings
                : DynamoDbSnapshotStoreSettings.Create(config);

            _client = DynamoDbSetup.InitClient(_settings);
        }

        protected override void PreStart()
        {
            base.PreStart();

            if (!_settings.AutoInitialize)
            {
                var builder = new TableBuilder(_client, new TableConfig(_settings.TableName));
                _table = builder.Build();

                return;
            }

            Initialize().PipeTo(Self);
            BecomeStacked(WaitingForInitialization);
        }

        public IStash? Stash { get; set; }

        protected override async Task<SelectedSnapshot> LoadAsync(string persistenceId,
            SnapshotSelectionCriteria criteria,
            CancellationToken cancellationToken)
        {
            var filter = new QueryFilter();
            filter.AddCondition(SnapshotDocument.Keys.PersistenceId, QueryOperator.Equal, persistenceId);
            filter.AddCondition(SnapshotDocument.Keys.SequenceNumber, QueryOperator.Between, criteria.MinSequenceNr,
                criteria.MaxSequenceNr);

            var search = _table!.Query(new QueryOperationConfig
            {
                BackwardSearch = true,
                CollectResults = false,
                Filter = filter,
                ConsistentRead = true
            });

            while (!search.IsDone)
            {
                var items = await search.GetNextSetAsync(cancellationToken);
                var document = items.FirstOrDefault(x =>
                    x.TryGetLong(SnapshotDocument.Keys.Timestamp, out var longValue) &&
                    longValue >= (criteria.MinTimestamp ?? DateTime.MinValue).Ticks &&
                    longValue <= criteria.MaxTimeStamp.Ticks);

                if (document != null)
                {
                    return new SnapshotDocument(document)
                        .ToSelectedSnapshot(_actorSystem);
                }
            }

            return null;
        }

        /// <inheritdoc />
        protected override async Task SaveAsync(SnapshotMetadata metadata, object snapshot,
            CancellationToken cancellationToken)
        {
            await _table!.PutItemAsync(SnapshotDocument.ToDocument(metadata, snapshot, _actorSystem),
                cancellationToken);
        }

        /// <inheritdoc />
        protected override async Task DeleteAsync(SnapshotMetadata metadata, CancellationToken cancellationToken)
        {
            var document = await _table!.GetItemAsync(metadata.PersistenceId, metadata.SequenceNr, cancellationToken);

            if (document is null || !document.Any())
            {
                return;
            }

            var snapshotDocument = new SnapshotDocument(document);
            if (metadata.Timestamp == DateTime.MinValue || snapshotDocument.Timestamp <= metadata.Timestamp.Ticks)
            {
                await _table.DeleteItemAsync(document, cancellationToken);
            }
        }

        /// <inheritdoc />
        protected override async Task DeleteAsync(string persistenceId, SnapshotSelectionCriteria criteria,
            CancellationToken cancellationToken)
        {
            var filter = new QueryFilter();
            filter.AddCondition(SnapshotDocument.Keys.PersistenceId, QueryOperator.Equal, persistenceId);
            filter.AddCondition(SnapshotDocument.Keys.SequenceNumber, QueryOperator.Between, criteria.MinSequenceNr,
                criteria.MaxSequenceNr);

            var search = _table!.Query(persistenceId, filter);

            while (!search.IsDone)
            {
                var items = await search.GetNextSetAsync(cancellationToken);

                var batch = _table.CreateBatchWrite();

                foreach (var item in items)
                {
                    var snapshotDocument = new SnapshotDocument(item);

                    if ((!criteria.MinTimestamp.HasValue || criteria.MinTimestamp.Value == DateTime.MinValue ||
                         snapshotDocument.Timestamp >= criteria.MinTimestamp.Value.Ticks) &&
                        snapshotDocument.Timestamp <= criteria.MaxTimeStamp.Ticks)
                    {
                        batch.AddItemToDelete(item);
                    }
                }

                await batch.ExecuteAsync(cancellationToken);
            }
        }

        private async Task<object> Initialize()
        {
            try
            {
                await _client.EnsureTableExistsWithDefinition(
                    _settings.TableName,
                    new List<AttributeDefinition>
                    {
                        new(SnapshotDocument.Keys.PersistenceId, ScalarAttributeType.S),
                        new(SnapshotDocument.Keys.SequenceNumber, ScalarAttributeType.N)
                    }.ToImmutableList(),
                    new List<KeySchemaElement>
                    {
                        new(SnapshotDocument.Keys.PersistenceId, KeyType.HASH),
                        new(SnapshotDocument.Keys.SequenceNumber, KeyType.RANGE)
                    }.ToImmutableList(),
                    ImmutableList<GlobalSecondaryIndex>.Empty);

                var builder = new TableBuilder(_client, new TableConfig(_settings.TableName));
                _table = builder.Build();

                return Events.Initialized.Instance;
            }
            catch (Exception e)
            {
                return new Status.Failure(e);
            }
        }

        private bool WaitingForInitialization(object message)
        {
            switch (message)
            {
                case Events.Initialized:
                    UnbecomeStacked();
                    Stash?.UnstashAll();
                    return true;

                case Status.Failure failure:
                    _log.Error(failure.Cause, "Error during snapshot store initialization");
                    Context.Stop(Self);
                    return true;

                //TODO: Remove once the obsolete Failure is removed
                case Failure failure:
                    _log.Error(failure.Exception, "Error during snapshot store initialization");
                    Context.Stop(Self);
                    return true;

                default:
                    Stash?.Stash();
                    return true;
            }
        }
    }
}