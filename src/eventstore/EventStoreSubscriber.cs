// <copyright file="EventStoreSubscriber.cs" company="Corsham Science">
// Copyright (c) Corsham Science. All rights reserved.
// </copyright>

namespace CorshamScience.MessageDispatch.EventStore
{
    using System;
    using System.Collections.Concurrent;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Timers;
    using CorshamScience.MessageDispatch.Core;
    using global::EventStore.Client;
    using Microsoft.Extensions.Logging;
    using Timer = System.Timers.Timer;

    /// <summary>
    /// Subscriber for event store.
    /// </summary>
    public class EventStoreSubscriber
    {
        private readonly Timer _liveProcessingTimer = new Timer(TimeSpan.FromMinutes(10).TotalMilliseconds);

        private readonly WriteThroughFileCheckpoint _checkpoint;
        private readonly object _subscriptionLock = new object();

        private EventStoreClient _eventStoreClient;
        private ulong? _startingPosition;
        private StreamSubscription _subscription;
        private string _streamName;
        private bool _liveOnly;

        private ulong _lastNonLiveEventNumber = ulong.MinValue;
        private ulong? _lastReceivedEventNumber;
        private int _eventsProcessed;
        private bool _catchingUp = true;

        private BlockingCollection<ResolvedEvent> _queue;
        private IDispatcher<ResolvedEvent> _dispatcher;
        private ulong _lastDispatchedEventNumber;

        private ILogger _logger;

        private EventStoreSubscriber(
            EventStoreClient eventStoreClient,
            IDispatcher<ResolvedEvent> dispatcher,
            string streamName,
            ILogger logger,
            ulong? startingPosition,
            int upperQueueBound = 2048)
            => Init(eventStoreClient, dispatcher, streamName, logger, startingPosition, upperQueueBound);

        private EventStoreSubscriber(
            EventStoreClient eventStoreClient,
            IDispatcher<ResolvedEvent> dispatcher,
            ILogger logger,
            string streamName,
            string checkpointFilePath,
            int upperQueueBound = 2048)
        {
            _checkpoint = new WriteThroughFileCheckpoint(checkpointFilePath, "lastProcessedPosition", false, ulong.MinValue);
            var initialCheckpointPosition = _checkpoint.Read();
            ulong? startingPosition = null;

            if (initialCheckpointPosition != ulong.MinValue)
            {
                startingPosition = initialCheckpointPosition;
            }

            Init(eventStoreClient, dispatcher, streamName, logger, startingPosition, upperQueueBound);
        }

        private EventStoreSubscriber(
            EventStoreClient eventStoreClient,
            IDispatcher<ResolvedEvent> dispatcher,
            string streamName,
            ILogger logger,
            int upperQueueBound = 2048)
            => Init(eventStoreClient, dispatcher, streamName, logger, upperQueueBound: upperQueueBound, liveOnly: true);

        /// <summary>
        /// Gets a new catchup progress object.
        /// </summary>
        // ReSharper disable once UnusedMember.Global
        public CatchupProgress CatchUpPercentage
        {
            get
            {
                var lastStreamPosition = _eventStoreClient.ReadStreamAsync(
                    Direction.Backwards,
                    _streamName,
                    StreamPosition.End,
                    maxCount: 1,
                    resolveLinkTos: false).LastAsync().Result.OriginalEventNumber.ToUInt64();

                return new CatchupProgress(_eventsProcessed, _startingPosition ?? 0, _streamName, lastStreamPosition);
            }
        }

        /// <summary>
        /// Gets a value indicating whether the view model is ready or not.
        /// </summary>
        public bool ViewModelsReady => !_catchingUp && _lastDispatchedEventNumber >= _lastNonLiveEventNumber;

        /// <summary>
        /// Creates a live eventstore subscription.
        /// </summary>
        /// <param name="eventStoreClient">Eventstore connection.</param>
        /// <param name="dispatcher">Dispatcher.</param>
        /// <param name="streamName">Stream name to push events into.</param>
        /// <param name="logger">Logger.</param>
        /// <param name="upperQueueBound">Upper Queue Bound.</param>
        /// <returns>A new EventStoreSubscriber object.</returns>
        // ReSharper disable once UnusedMember.Global
        public static EventStoreSubscriber CreateLiveSubscription(
            EventStoreClient eventStoreClient,
            IDispatcher<ResolvedEvent> dispatcher,
            string streamName,
            ILogger logger,
            int upperQueueBound = 2048)
            => new EventStoreSubscriber(eventStoreClient, dispatcher, streamName, logger, upperQueueBound);

#pragma warning disable CS0618 // Type or member is obsolete
        /// <summary>
        /// Creates an eventstore catchup subscription using a checkpoint file.
        /// </summary>
        /// <param name="eventStoreClient">Eventstore connection.</param>
        /// <param name="dispatcher">Dispatcher.</param>
        /// <param name="streamName">Stream name to push events into.</param>
        /// <param name="logger">Logger.</param>
        /// <param name="checkpointFilePath">Path of the checkpoint file.</param>
        /// <param name="upperQueueBound">Upper Queue Bound.</param>
        /// <returns>A new EventStoreSubscriber object.</returns>
        // ReSharper disable once UnusedMember.Global
        public static EventStoreSubscriber CreateCatchupSubscriptionUsingCheckpoint(
            EventStoreClient eventStoreClient,
            IDispatcher<ResolvedEvent> dispatcher,
            string streamName,
            ILogger logger,
            string checkpointFilePath,
            int upperQueueBound = 2048)
            => new EventStoreSubscriber(eventStoreClient, dispatcher, logger, streamName, checkpointFilePath, upperQueueBound);

        /// <summary>
        /// Creates an ecventstore catchup subscription from a position.
        /// </summary>
        /// <param name="eventStoreClient">Eventstore connection.</param>
        /// <param name="dispatcher">Dispatcher.</param>
        /// <param name="streamName">Stream name to push events into.</param>
        /// <param name="logger">Logger.</param>
        /// <param name="startingPosition">Starting Position.</param>
        /// <param name="upperQueueBound">Upper Queue Bound.</param>
        /// <returns>A new EventStoreSubscriber object.</returns>
        // ReSharper disable once UnusedMember.Global
        public static EventStoreSubscriber CreateCatchupSubscriptionFromPosition(
            EventStoreClient eventStoreClient,
            IDispatcher<ResolvedEvent> dispatcher,
            string streamName,
            ILogger logger,
            ulong? startingPosition,
            int upperQueueBound = 2048)
            => new EventStoreSubscriber(eventStoreClient, dispatcher, streamName, logger, startingPosition, upperQueueBound);
#pragma warning restore CS0618 // Type or member is obsolete

        /// <summary>
        /// Start the subscriber.
        /// </summary>
        /// <param name="restart">Starting from a restart.</param>
        public void Start(bool restart = false)
        {
            lock (_subscriptionLock)
            {
                KillSubscription();

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

            if (restart)
            {
                return;
            }

            var processor = new Thread(ProcessEvents) { IsBackground = true };
            processor.Start();
        }

        /// <summary>
        /// Shut down the subscription.
        /// </summary>
        // ReSharper disable once UnusedMember.Global
        public void ShutDown()
        {
            lock (_subscriptionLock)
            {
                _subscription.Dispose();
            }
        }

        private void Init(
            EventStoreClient connection,
            IDispatcher<ResolvedEvent> dispatcher,
            string streamName,
            ILogger logger,
            ulong? startingPosition = null,
            int upperQueueBound = 2048,
            bool liveOnly = false)
        {
            _liveProcessingTimer.Elapsed += LiveProcessingTimerOnElapsed;
            _logger = logger;
            _eventsProcessed = 0;
            _startingPosition = startingPosition;
            _lastReceivedEventNumber = startingPosition;
            _dispatcher = dispatcher;
            _streamName = streamName;
            _eventStoreClient = connection;
            _liveOnly = liveOnly;

            _queue = new BlockingCollection<ResolvedEvent>(upperQueueBound);
        }

        private void RestartSubscription()
        {
            lock (_subscriptionLock)
            {
                KillSubscription();

                _startingPosition = _lastReceivedEventNumber;
                _lastNonLiveEventNumber = ulong.MinValue;
                _catchingUp = true;
                Start(true);
            }

            lock (_liveProcessingTimer)
            {
                if (!_liveProcessingTimer.Enabled)
                {
                    _liveProcessingTimer.Start();
                }
            }
        }

        private void LiveProcessingTimerOnElapsed(object sender, ElapsedEventArgs elapsedEventArgs) => _logger.LogError("Event Store Subscription has been down for 10 minutes");

        private void ProcessEvents()
        {
            foreach (var item in _queue.GetConsumingEnumerable())
            {
                ProcessEvent(item);
                _lastDispatchedEventNumber = item.OriginalEventNumber.ToUInt64();
            }
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

            RestartSubscription();
        }

        private Task EventAppeared(ResolvedEvent resolvedEvent)
        {
            if (_catchingUp)
            {
                _lastNonLiveEventNumber = resolvedEvent.OriginalEventNumber.ToUInt64();
            }

            _queue.Add(resolvedEvent);
            _lastReceivedEventNumber = resolvedEvent.OriginalEventNumber.ToUInt64();

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

                _checkpoint.Write(resolvedEvent.OriginalEventNumber.ToUInt64());
                _checkpoint.Flush();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error dispatching event from Event Store subscriber ({0}/{1})", resolvedEvent.Event.EventStreamId, resolvedEvent.Event.EventNumber);
            }
        }

        private void KillSubscription()
        {
            _subscription.Dispose();
            _subscription = null;
        }
    }
}
