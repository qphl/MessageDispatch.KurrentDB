// <copyright file="EventStoreSubscriber.cs" company="Corsham Science">
// Copyright (c) Corsham Science. All rights reserved.
// </copyright>

namespace CorshamScience.MessageDispatch.EventStore
{
    using System;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using CorshamScience.MessageDispatch.Core;
    using global::EventStore.Client;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// Subscriber for event store.
    /// </summary>
    public class EventStoreSubscriber
    {
        private const string AllStreamName = "$all";
        private readonly WriteThroughFileCheckpoint _checkpoint;
        private readonly object _subscriptionLock = new object();

        private EventStoreClient _eventStoreClient;
        private ulong? _startingPosition;
        private StreamSubscription _subscription;
        private string _streamName;
        private bool _liveOnly;
        private bool _isSubscribed;
        private bool _subscribeToAll;
        private ulong? _lastProcessedEventPosition;
        private ulong _actualEndOfStreamPosition;
        private ulong _liveEventThreshold;
        private ulong _liveThresholdPosition;
        private DateTime _lastStreamPositionTimestamp;
        private Func<Task> _setLastPositions;

        private IDispatcher<ResolvedEvent> _dispatcher;
        private ILogger _logger;

        private EventStoreSubscriber(
            EventStoreClient eventStoreClient,
            IDispatcher<ResolvedEvent> dispatcher,
            string streamName,
            ILogger logger,
            ulong? startingPosition,
            ulong liveEventThreshold)
            => Init(eventStoreClient, dispatcher, streamName, logger, liveEventThreshold, startingPosition);

        private EventStoreSubscriber(
            EventStoreClient eventStoreClient,
            IDispatcher<ResolvedEvent> dispatcher,
            ILogger logger,
            string streamName,
            string checkpointFilePath,
            ulong liveEventThreshold)
        {
            _checkpoint = new WriteThroughFileCheckpoint(checkpointFilePath, "lastProcessedPosition", false, -1);
            var initialCheckpointPosition = _checkpoint.Read();
            ulong? startingPosition = null;

            if (initialCheckpointPosition != -1)
            {
                startingPosition = (ulong)initialCheckpointPosition;
            }

            Init(eventStoreClient, dispatcher, streamName, logger, liveEventThreshold, startingPosition);
        }

        private EventStoreSubscriber(
            EventStoreClient eventStoreClient,
            IDispatcher<ResolvedEvent> dispatcher,
            string streamName,
            ILogger logger,
            ulong liveEventThreshold)
            => Init(eventStoreClient, dispatcher, streamName, logger, liveEventThreshold, liveOnly: true);

        /// <summary>
        /// Gets a new catchup progress object.
        /// </summary>
        // ReSharper disable once UnusedMember.Global
        public CatchupProgress CatchupProgress
        {
            get
            {
                var lastStreamPosition = GetLastPositions().Result;

                return new CatchupProgress(
                    _lastProcessedEventPosition ?? 0,
                    _streamName,
                    lastStreamPosition.actualEndOfStreamPosition,
                    _startingPosition ?? 0,
                    _subscribeToAll);
            }
        }

        /// <summary>
        /// Gets a value indicating whether the view model is ready or not.
        /// </summary>
        /// <returns>Returns true if catchup is within threshold.</returns>
        public bool IsLive
        {
            get
            {
                var lastStreamPosition = GetLastPositions().Result;

                return (_liveOnly && _lastProcessedEventPosition is null && _isSubscribed) ||
                       _lastProcessedEventPosition >= lastStreamPosition.liveThresholdPosition;
            }
        }

        /// <summary>
        /// Creates a live eventstore subscription.
        /// </summary>
        /// <param name="eventStoreClient">Eventstore connection.</param>
        /// <param name="dispatcher">Dispatcher.</param>
        /// <param name="streamName">Stream name to push events into.</param>
        /// <param name="logger">Logger.</param>
        /// <param name="liveEventThreshold">Proximity to end of stream before subscription considered live.</param>
        /// <returns>A new EventStoreSubscriber object.</returns>
        // ReSharper disable once UnusedMember.Global
        public static EventStoreSubscriber CreateLiveSubscription(
            EventStoreClient eventStoreClient,
            IDispatcher<ResolvedEvent> dispatcher,
            string streamName,
            ILogger logger,
            ulong liveEventThreshold = 10)
            => new EventStoreSubscriber(eventStoreClient, dispatcher, streamName, logger, liveEventThreshold);

        /// <summary>
        /// Creates an eventstore catchup subscription using a checkpoint file.
        /// </summary>
        /// <param name="eventStoreClient">Eventstore connection.</param>
        /// <param name="dispatcher">Dispatcher.</param>
        /// <param name="streamName">Stream name to push events into.</param>
        /// <param name="logger">Logger.</param>
        /// <param name="checkpointFilePath">Path of the checkpoint file.</param>
        /// <param name="liveEventThreshold">Proximity to end of stream before subscription considered live.</param>
        /// <returns>A new EventStoreSubscriber object.</returns>
        // ReSharper disable once UnusedMember.Global
        public static EventStoreSubscriber CreateCatchupSubscriptionUsingCheckpoint(
            EventStoreClient eventStoreClient,
            IDispatcher<ResolvedEvent> dispatcher,
            string streamName,
            ILogger logger,
            string checkpointFilePath,
            ulong liveEventThreshold = 10)
            => new EventStoreSubscriber(eventStoreClient, dispatcher, logger, streamName, checkpointFilePath, liveEventThreshold);

        /// <summary>
        /// Creates an eventstore catchup subscription from a position.
        /// </summary>
        /// <param name="eventStoreClient">Eventstore connection.</param>
        /// <param name="dispatcher">Dispatcher.</param>
        /// <param name="streamName">Stream name to push events into.</param>
        /// <param name="logger">Logger.</param>
        /// <param name="startingPosition">Starting Position.</param>
        /// <param name="liveEventThreshold">Proximity to end of stream before subscription considered live.</param>
        /// <returns>A new EventStoreSubscriber object.</returns>
        // ReSharper disable once UnusedMember.Global
        public static EventStoreSubscriber CreateCatchupSubscriptionFromPosition(
            EventStoreClient eventStoreClient,
            IDispatcher<ResolvedEvent> dispatcher,
            string streamName,
            ILogger logger,
            ulong? startingPosition,
            ulong liveEventThreshold = 10)
            => new EventStoreSubscriber(eventStoreClient, dispatcher, streamName, logger, startingPosition, liveEventThreshold);

        /// <summary>
        /// Creates an eventstore catchup subscription that is subscribed to all from the start.
        /// </summary>
        /// <param name="eventStoreClient">Eventstore connection.</param>
        /// <param name="dispatcher">Dispatcher.</param>
        /// <param name="logger">Logger.</param>
        /// <param name="liveEventThreshold">Proximity to end of stream before subscription considered live.</param>
        /// <returns>A new EventStoreSubscriber object.</returns>
        // ReSharper disable once UnusedMember.Global
        public static EventStoreSubscriber CreateCatchupSubscriptionSubscribedToAll(
            EventStoreClient eventStoreClient,
            IDispatcher<ResolvedEvent> dispatcher,
            ILogger logger,
            ulong liveEventThreshold = 10)
            => new EventStoreSubscriber(
                eventStoreClient,
                dispatcher,
                AllStreamName,
                logger,
                liveEventThreshold);

        /// <summary>
        /// Creates an eventstore catchup subscription that is subscribed to all from a position.
        /// </summary>
        /// <param name="eventStoreClient">Eventstore connection.</param>
        /// <param name="dispatcher">Dispatcher.</param>
        /// <param name="logger">Logger.</param>
        /// <param name="startingPosition">Starting Position.</param>
        /// <param name="liveEventThreshold">Proximity to end of stream before subscription considered live.</param>
        /// <returns>A new EventStoreSubscriber object.</returns>
        // ReSharper disable once UnusedMember.Global
        public static EventStoreSubscriber CreateCatchupSubscriptionSubscribedToAllFromPosition(
            EventStoreClient eventStoreClient,
            IDispatcher<ResolvedEvent> dispatcher,
            ILogger logger,
            ulong? startingPosition,
            ulong liveEventThreshold = 10)
            => new EventStoreSubscriber(
                eventStoreClient,
                dispatcher,
                AllStreamName,
                logger,
                startingPosition,
                liveEventThreshold);

        /// <summary>
        /// Creates an eventstore catchup subscription subscribed to all using a checkpoint file.
        /// </summary>
        /// <param name="eventStoreClient">Eventstore connection.</param>
        /// <param name="dispatcher">Dispatcher.</param>
        /// <param name="logger">Logger.</param>
        /// <param name="checkpointFilePath">Path of the checkpoint file.</param>
        /// <param name="liveEventThreshold">Proximity to end of stream before subscription considered live.</param>
        /// <returns>A new EventStoreSubscriber object.</returns>
        // ReSharper disable once UnusedMember.Global
        public static EventStoreSubscriber CreateCatchupSubscriptionSubscribedToAllUsingCheckpoint(
            EventStoreClient eventStoreClient,
            IDispatcher<ResolvedEvent> dispatcher,
            ILogger logger,
            string checkpointFilePath,
            ulong liveEventThreshold = 10)
            => new EventStoreSubscriber(
                    eventStoreClient,
                    dispatcher,
                    logger,
                    AllStreamName,
                    checkpointFilePath,
                    liveEventThreshold);

        /// <summary>
        /// Start the subscriber.
        /// </summary>
        // ReSharper disable once MemberCanBePrivate.Global
        public void Start()
        {
            while (true)
            {
                _isSubscribed = false;

                try
                {
                    Monitor.Enter(_subscriptionLock);

                    KillSubscription();

                    // No synchronization context is needed to disable synchronization context.
                    // That enables running asynchronous method not causing deadlocks.
                    // As this is a background process then we don't need to have async context here.
                    using (NoSynchronizationContextScope.Enter())
                    {
                        var filterOptions = new SubscriptionFilterOptions(
                            EventTypeFilter.ExcludeSystemEvents(),
                            checkpointReached: CheckpointReached);
                        const bool resolveLinkTos = true;

                        Task Appeared(
                            StreamSubscription streamSubscription,
                            ResolvedEvent e,
                            CancellationToken cancellationToken) =>
                            EventAppeared(e);

                        switch (_liveOnly)
                        {
                            case true when !_subscribeToAll:
                                _subscription = _eventStoreClient.SubscribeToStreamAsync(
                                    _streamName,
                                    FromStream.End,
                                    Appeared,
                                    resolveLinkTos,
                                    SubscriptionDropped).Result;
                                break;
                            case false when !_subscribeToAll:
                            {
                                var fromStream = _startingPosition.HasValue ?
                                    FromStream.After(new StreamPosition(_startingPosition.Value)) :
                                    FromStream.Start;

                                _subscription = _eventStoreClient.SubscribeToStreamAsync(
                                    _streamName,
                                    fromStream,
                                    Appeared,
                                    resolveLinkTos,
                                    SubscriptionDropped).Result;
                                break;
                            }

                            case true when _subscribeToAll:
                                _subscription = _eventStoreClient.SubscribeToAllAsync(
                                        FromAll.End,
                                        Appeared,
                                        resolveLinkTos,
                                        SubscriptionDropped,
                                        filterOptions)
                                    .Result;
                                break;
                            case false when _subscribeToAll:
                                var fromAll = _startingPosition.HasValue ?
                                    FromAll.After(new Position(_startingPosition.Value, _startingPosition.Value)) :
                                    FromAll.Start;

                                _subscription = _eventStoreClient.SubscribeToAllAsync(
                                        fromAll,
                                        Appeared,
                                        resolveLinkTos,
                                        SubscriptionDropped,
                                        filterOptions)
                                    .Result;
                                break;
                        }
                    }

                    _isSubscribed = true;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to resubscribe to '{StreamName}' dropped with '{ExceptionMessage}{ExceptionStackTrace}'", _streamName, ex.Message, ex.StackTrace);
                }
                finally
                {
                    Monitor.Exit(_subscriptionLock);
                }

                if (_isSubscribed)
                {
                    break;
                }

                // Sleep between reconnections to not flood the database or not kill the CPU with infinite loop
                // Randomness added to reduce the chance of multiple subscriptions trying to reconnect at the same time
                Thread.Sleep(1000 + new Random((int)DateTime.UtcNow.Ticks).Next(1000));
            }
        }

        /// <summary>
        /// Shut down the subscription.
        /// </summary>
        // ReSharper disable once UnusedMember.Global
        public void ShutDown()
        {
            lock (_subscriptionLock)
            {
                KillSubscription();
            }
        }

        private void Init(
            EventStoreClient connection,
            IDispatcher<ResolvedEvent> dispatcher,
            string streamName,
            ILogger logger,
            ulong liveEventThreshold,
            ulong? startingPosition = null,
            bool liveOnly = false)
        {
            _logger = logger;
            _startingPosition = startingPosition;
            _lastProcessedEventPosition = startingPosition;
            _dispatcher = dispatcher;
            _streamName = streamName;
            _eventStoreClient = connection;
            _liveOnly = liveOnly;
            _subscribeToAll = streamName == AllStreamName;
            _liveEventThreshold = liveEventThreshold;
            _liveThresholdPosition = StreamPosition.End;
            _lastStreamPositionTimestamp = DateTime.MinValue;

            _setLastPositions = _subscribeToAll
                ? async () =>
                {
                    var eventsWithinThreshold = await _eventStoreClient.ReadAllAsync(
                            Direction.Backwards,
                            Position.End,
                            maxCount: (long)_liveEventThreshold)
                        .ToListAsync();

                    _liveThresholdPosition = eventsWithinThreshold.Last().OriginalEvent.Position.CommitPosition;
                    _actualEndOfStreamPosition = eventsWithinThreshold.First().OriginalEvent.Position.CommitPosition;
                }
                : async () =>
                {
                    var eventsWithinThreshold = await _eventStoreClient.ReadStreamAsync(
                            Direction.Backwards,
                            _streamName,
                            StreamPosition.End,
                            maxCount: (long)_liveEventThreshold,
                            resolveLinkTos: false)
                        .ToListAsync();

                    _liveThresholdPosition = eventsWithinThreshold.Last().OriginalEventNumber.ToUInt64();
                    _actualEndOfStreamPosition = eventsWithinThreshold.First().OriginalEventNumber.ToUInt64();
                };
        }

        private void SubscriptionDropped(StreamSubscription eventStoreCatchUpSubscription, SubscriptionDroppedReason subscriptionDropReason, Exception ex)
        {
            if (ex != null)
            {
                _logger.LogInformation(ex, "Event Store subscription dropped {0}", subscriptionDropReason.ToString());
            }
            else
            {
                _logger.LogInformation("Event Store subscription dropped {0}", subscriptionDropReason.ToString());
            }

            if (subscriptionDropReason == SubscriptionDroppedReason.Disposed)
            {
                _logger.LogInformation("Not attempting to restart subscription was disposed. Subscription is dead.");
                return;
            }

            _isSubscribed = false;
            _startingPosition = _lastProcessedEventPosition;
            Start();
        }

        private Task EventAppeared(ResolvedEvent resolvedEvent)
        {
            ProcessEvent(resolvedEvent);

            var lastProcessedEventPosition = GetLastProcessedPosition(resolvedEvent);

            if (_liveOnly && _lastProcessedEventPosition is null)
            {
                _startingPosition = lastProcessedEventPosition;
            }

            _lastProcessedEventPosition = lastProcessedEventPosition;

            return Task.CompletedTask;
        }

        private void ProcessEvent(ResolvedEvent resolvedEvent)
        {
            if (resolvedEvent.Event == null || resolvedEvent.Event.EventType.StartsWith("$"))
            {
                return;
            }

            try
            {
                _dispatcher.Dispatch(resolvedEvent);

                var checkpointNumber = GetLastProcessedPosition(resolvedEvent);

                WriteCheckpoint(checkpointNumber);
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Error dispatching event from Event Store subscriber ({0}/{1})",
                    resolvedEvent.Event.EventStreamId,
                    resolvedEvent.Event.EventNumber);
            }
        }

        private ulong GetLastProcessedPosition(ResolvedEvent resolvedEvent)
        {
            return _subscribeToAll
                ? resolvedEvent.OriginalEvent.Position.CommitPosition
                : resolvedEvent.OriginalEventNumber.ToUInt64();
        }

        private void WriteCheckpoint(ulong checkpointNumber)
        {
            if (_checkpoint == null)
            {
                return;
            }

            if (checkpointNumber > long.MaxValue)
            {
                _logger.LogError(
                    "Value is too large to be checkpointed. Checkpoint number {CheckpointNumber}",
                    checkpointNumber);
                return;
            }

            _checkpoint.Write((long)checkpointNumber);
            _checkpoint.Flush();
        }

        private async Task<(ulong liveThresholdPosition, ulong actualEndOfStreamPosition)> GetLastPositions()
        {
            var streamPositionIsStale = (DateTime.UtcNow - _lastStreamPositionTimestamp) > TimeSpan.FromSeconds(10);

            if (_isSubscribed && streamPositionIsStale)
            {
                await _setLastPositions();
                _lastStreamPositionTimestamp = DateTime.UtcNow;
            }

            return (_liveThresholdPosition, _actualEndOfStreamPosition);
        }

        private void KillSubscription()
        {
            if (_subscription != null)
            {
                _subscription.Dispose();
                _subscription = null;
            }

            _isSubscribed = false;
        }

        private Task CheckpointReached(
            StreamSubscription streamSubscription,
            Position position,
            CancellationToken cancellationToken)
        {
            _lastProcessedEventPosition = position.CommitPosition;
            WriteCheckpoint((ulong)_lastProcessedEventPosition);

            return Task.CompletedTask;
        }
    }
}
