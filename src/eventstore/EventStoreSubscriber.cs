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
        private readonly WriteThroughFileCheckpoint _checkpoint;
        private readonly object _subscriptionLock = new object();

        private EventStoreClient _eventStoreClient;
        private ulong? _startingPosition;
        private StreamSubscription _subscription;
        private string _streamName;
        private bool _liveOnly;
        private bool _isSubscribed;
        private bool _subscribeToAll;
        private ulong? _lastProcessedEventNumber;
        private int _eventsProcessed;
        private ulong _liveEventThreshold;
        private ulong _lastStreamPosition;
        private DateTime _lastStreamPositionTimestamp;

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
        public CatchupProgress CatchUpPercentage
        {
            get
            {
                var lastStreamPosition = GetLastStreamPosition();
                return new CatchupProgress(_eventsProcessed, _startingPosition ?? 0, _streamName, lastStreamPosition);
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
                if (!_isSubscribed)
                {
                    return false;
                }

                var catchUpProgress = CatchUpPercentage;
                var currentPosition = catchUpProgress.StartPosition + (ulong)catchUpProgress.EventsProcessed;
                var currentPositionFromEnd = catchUpProgress.TotalEvents - currentPosition;

                return currentPositionFromEnd < _liveEventThreshold;
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
        /// Creates an eventstore catchup subscription that is subscribed to all.
        /// </summary>
        /// <param name="eventStoreClient">Eventstore connection.</param>
        /// <param name="dispatcher">Dispatcher.</param>
        /// <param name="logger">Logger.</param>
        /// <param name="startingPosition">Starting Position.</param>
        /// <param name="liveEventThreshold">Proximity to end of stream before subscription considered live.</param>
        /// <returns>A new EventStoreSubscriber object.</returns>
        // ReSharper disable once UnusedMember.Global
        public static EventStoreSubscriber CreateCatchupSubscriptionSubscribedToAll(
            EventStoreClient eventStoreClient,
            IDispatcher<ResolvedEvent> dispatcher,
            ILogger logger,
            ulong? startingPosition,
            ulong liveEventThreshold = 10)
            => new EventStoreSubscriber(
                eventStoreClient,
                dispatcher,
                null,
                logger,
                startingPosition,
                liveEventThreshold);

        /// <summary>
        /// Start the subscriber.
        /// </summary>
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
                        if (_liveOnly)
                        {
                            _subscription = _eventStoreClient.SubscribeToStreamAsync(
                                _streamName,
                                FromStream.End,
                                (s, e, t) => EventAppeared(e),
                                resolveLinkTos: true,
                                subscriptionDropped: SubscriptionDropped).Result;
                        }
                        else
                        {
                            var fromStream = _startingPosition.HasValue ?
                                FromStream.After(new StreamPosition(_startingPosition.Value)) :
                                FromStream.Start;

                            _subscription = _eventStoreClient.SubscribeToStreamAsync(
                                   _streamName,
                                   fromStream,
                                   (s, e, t) => EventAppeared(e),
                                   resolveLinkTos: true,
                                   subscriptionDropped: SubscriptionDropped).Result;
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
            _eventsProcessed = 0;
            _startingPosition = startingPosition;
            _lastProcessedEventNumber = startingPosition;
            _dispatcher = dispatcher;
            _streamName = streamName;
            _eventStoreClient = connection;
            _liveOnly = liveOnly;
            _subscribeToAll = string.IsNullOrWhiteSpace(streamName);
            _liveEventThreshold = liveEventThreshold;
            _lastStreamPosition = StreamPosition.End;
            _lastStreamPositionTimestamp = DateTime.MinValue;
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
            _startingPosition = _lastProcessedEventNumber;
            Start();
        }

        private Task EventAppeared(ResolvedEvent resolvedEvent)
        {
            ProcessEvent(resolvedEvent);
            _lastProcessedEventNumber = resolvedEvent.OriginalEventNumber.ToUInt64();

            return Task.CompletedTask;
        }

        private void ProcessEvent(ResolvedEvent resolvedEvent)
        {
            _eventsProcessed++;
            if (resolvedEvent.Event == null || resolvedEvent.Event.EventType.StartsWith("$"))
            {
                return;
            }

            try
            {
                _dispatcher.Dispatch(resolvedEvent);

                if (_checkpoint == null)
                {
                    return;
                }

                if (resolvedEvent.OriginalEventNumber.ToUInt64() > long.MaxValue)
                {
                    _logger.LogError("Event number is too large to be checkpointed. Event number: {EventNumber}", resolvedEvent.OriginalEventNumber);
                    return;
                }
                _checkpoint.Write(resolvedEvent.OriginalEventNumber.ToInt64());
                _checkpoint.Flush();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error dispatching event from Event Store subscriber ({0}/{1})", resolvedEvent.Event.EventStreamId, resolvedEvent.Event.EventNumber);
            }
        }

        private ulong GetLastStreamPosition()
        {
            var streamPositionIsStale = (DateTime.UtcNow - _lastStreamPositionTimestamp) > TimeSpan.FromSeconds(10);

            if (_isSubscribed && streamPositionIsStale)
            {
                _lastStreamPosition = _eventStoreClient.ReadStreamAsync(
                    Direction.Backwards,
                    _streamName,
                    StreamPosition.End,
                    maxCount: 1,
                    resolveLinkTos: false).LastAsync().Result.OriginalEventNumber.ToUInt64();

                _lastStreamPositionTimestamp = DateTime.UtcNow;
            }

            return _lastStreamPosition;
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
    }
}
