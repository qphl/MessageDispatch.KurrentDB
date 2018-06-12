// <copyright file="EventStoreSubscriber.cs" company="Cognisant">
// Copyright (c) Cognisant. All rights reserved.
// </copyright>

namespace CR.MessageDispatch.EventStore
{
    using System;
    using System.Collections.Concurrent;
    using System.Linq;
    using System.Threading;
    using System.Timers;
    using Core;
    using global::EventStore.ClientAPI;
    using Timer = System.Timers.Timer;

    /// <summary>
    /// Subscriber for event store.
    /// </summary>
    public class EventStoreSubscriber
    {
        private const string HeartbeatEventType = "SubscriberHeartbeat";

        private readonly Timer _liveProcessingTimer = new Timer(TimeSpan.FromMinutes(10).TotalMilliseconds);
        private readonly string _heartbeatStreamName = $"SubscriberHeartbeat-{Guid.NewGuid()}";

        private readonly WriteThroughFileCheckpoint _checkpoint;
        private readonly object _subscriptionLock = new object();

        private IEventStoreConnection _connection;
        private long? _startingPosition;
        private int _maxLiveQueueSize;
        private object _subscription;
        private int _catchupPageSize;
        private string _streamName;
        private bool _liveOnly;

        private long _lastNonLiveEventNumber = long.MinValue;
        private long? _lastReceivedEventNumber;
        private int _eventsProcessed;
        private bool _catchingUp = true;

        private TimeSpan _heartbeatTimeout;
        private DateTime _lastHeartbeat;
        private Timer _heartbeatTimer;
        private bool _usingHeartbeats;

        private BlockingCollection<ResolvedEvent> _queue;
        private IDispatcher<ResolvedEvent> _dispatcher;
        private long _lastDispatchedEventNumber;

        private ILogger _logger;

        /// <summary>
        /// Initializes a new instance of the <see cref="EventStoreSubscriber"/> class
        /// </summary>
        /// <param name="connection">Eventstore connection.</param>
        /// <param name="dispatcher">Dispatcher</param>
        /// <param name="streamName">Stream name to push events into</param>
        /// <param name="logger">Logger.</param>
        /// <param name="startingPosition">Starting Position.</param>
        /// <param name="catchUpPageSize">Catchup page size.</param>
        /// <param name="upperQueueBound">Upper Queue Bound</param>
        /// <param name="heartbeatFrequency">Frequency of heartbeat.</param>
        /// <param name="heartbeatTimeout">Timeout of heartbeat</param>
        /// <param name="maxLiveQueueSize">Maximum size of the live queue</param>
        [Obsolete("Please use new static method CreateCatchUpSubscirptionFromPosition, this constructor will be removed in the future.")]
        public EventStoreSubscriber(
            IEventStoreConnection connection,
            IDispatcher<ResolvedEvent> dispatcher,
            string streamName,
            ILogger logger,
            int? startingPosition,
            int catchUpPageSize = 1024,
            int upperQueueBound = 2048,
            TimeSpan? heartbeatFrequency = null,
            TimeSpan? heartbeatTimeout = null,
            int maxLiveQueueSize = 10000)
            => Init(connection, dispatcher, streamName, logger, heartbeatFrequency, heartbeatTimeout, startingPosition, catchUpPageSize, upperQueueBound, maxLiveQueueSize);

        /// <summary>
        /// Initializes a new instance of the <see cref="EventStoreSubscriber"/> class.
        /// </summary>
        /// <param name="connection">Eventstore connection.</param>
        /// <param name="dispatcher">Dispatcher</param>
        /// <param name="streamName">Stream name to push events into</param>
        /// <param name="logger">Logger.</param>
        /// <param name="checkpointFilePath">Path of the checkpoint file.</param>
        /// <param name="catchupPageSize">Catchup page size.</param>
        /// <param name="upperQueueBound">Upper Queue Bound</param>
        /// <param name="heartbeatFrequency">Frequency of heartbeat.</param>
        /// <param name="heartbeatTimeout">Timeout of heartbeat</param>
        /// <param name="maxLiveQueueSize">Maximum size of the live queue</param>
        [Obsolete("Please use new static method CreateCatchupSubscriptionUsingCheckpoint, this constructor will be removed in the future.")]
        public EventStoreSubscriber(
            IEventStoreConnection connection,
            IDispatcher<ResolvedEvent> dispatcher,
            ILogger logger,
            string streamName,
            string checkpointFilePath,
            int catchupPageSize = 1024,
            int upperQueueBound = 2048,
            TimeSpan? heartbeatFrequency = null,
            TimeSpan? heartbeatTimeout = null,
            int maxLiveQueueSize = 10000)
        {
            _checkpoint = new WriteThroughFileCheckpoint(checkpointFilePath, "lastProcessedPosition", false, -1);
            var initialCheckpointPosition = _checkpoint.Read();
            int? startingPosition = null;

            if (initialCheckpointPosition != -1)
            {
                startingPosition = (int)initialCheckpointPosition;
            }

            Init(connection, dispatcher, streamName, logger, heartbeatFrequency, heartbeatTimeout, startingPosition, catchupPageSize, upperQueueBound, maxLiveQueueSize);
        }

        private EventStoreSubscriber(
            IEventStoreConnection connection,
            IDispatcher<ResolvedEvent> dispatcher,
            string streamName,
            ILogger logger,
            int upperQueueBound = 2048,
            TimeSpan? heartbeatFrequency = null,
            TimeSpan? heartbeatTImeout = null)
            => Init(connection, dispatcher, streamName, logger, heartbeatFrequency, heartbeatTImeout, upperQueueBound: upperQueueBound, liveOnly: true);

        /// <summary>
        /// Gets a new catchup progress object.
        /// </summary>
        // ReSharper disable once UnusedMember.Global
        public CatchupProgress CatchUpPercentage
        {
            get
            {
                var totalEvents = _connection.ReadStreamEventsBackwardAsync(_streamName, StreamPosition.End, 1, true)
                    .Result.Events.First().OriginalEventNumber;

                return new CatchupProgress(_eventsProcessed, _startingPosition ?? 0, _streamName, totalEvents);
            }
        }

        /// <summary>
        /// Gets a value indicating whether the view model is ready or not.
        /// </summary>
        public bool ViewModelsReady => !_catchingUp && _lastDispatchedEventNumber >= _lastNonLiveEventNumber;

        /// <summary>
        /// Creates a live eventstore subscription.
        /// </summary>
        /// <param name="connection">Eventstore connection.</param>
        /// <param name="dispatcher">Dispatcher</param>
        /// <param name="streamName">Stream name to push events into</param>
        /// <param name="logger">Logger.</param>
        /// <param name="upperQueueBound">Upper Queue Bound</param>
        /// <param name="heartbeatFrequency">Frequency of heartbeat.</param>
        /// <param name="heartbeatTimeout">Timeout of heartbeat</param>
        /// <returns>A new EventStoreSubscriber object.</returns>
        // ReSharper disable once UnusedMember.Global
        public static EventStoreSubscriber CreateLiveSubscription(
            IEventStoreConnection connection,
            IDispatcher<ResolvedEvent> dispatcher,
            string streamName,
            ILogger logger,
            int upperQueueBound = 2048,
            TimeSpan? heartbeatFrequency = null,
            TimeSpan? heartbeatTimeout = null)
            => new EventStoreSubscriber(connection, dispatcher, streamName, logger, upperQueueBound, heartbeatFrequency, heartbeatTimeout);

#pragma warning disable CS0618 // Type or member is obsolete
        /// <summary>
        /// Creates an eventstore catchup subscription using a checkpoint file.
        /// </summary>
        /// <param name="connection">Eventstore connection.</param>
        /// <param name="dispatcher">Dispatcher</param>
        /// <param name="streamName">Stream name to push events into</param>
        /// <param name="logger">Logger.</param>
        /// <param name="checkpointFilePath">Path of the checkpoint file.</param>
        /// <param name="catchupPageSize">Catchup page size.</param>
        /// <param name="upperQueueBound">Upper Queue Bound</param>
        /// <param name="heartbeatFrequency">Frequency of heartbeat.</param>
        /// <param name="heartbeatTimeout">Timeout of heartbeat</param>
        /// <param name="maxLiveQueueSize">Maximum size of the live queue</param>
        /// <returns>A new EventStoreSubscriber object.</returns>
        // ReSharper disable once UnusedMember.Global
        public static EventStoreSubscriber CreateCatchupSubscriptionUsingCheckpoint(
            IEventStoreConnection connection,
            IDispatcher<ResolvedEvent> dispatcher,
            string streamName,
            ILogger logger,
            string checkpointFilePath,
            int catchupPageSize = 1024,
            int upperQueueBound = 2048,
            TimeSpan? heartbeatFrequency = null,
            TimeSpan? heartbeatTimeout = null,
            int maxLiveQueueSize = 10000)
            => new EventStoreSubscriber(connection, dispatcher, logger, streamName, checkpointFilePath, catchupPageSize, upperQueueBound, heartbeatFrequency, heartbeatTimeout, maxLiveQueueSize);

        /// <summary>
        /// Creates an ecventstore catchup subscription from a position.
        /// </summary>
        /// <param name="connection">Eventstore connection.</param>
        /// <param name="dispatcher">Dispatcher</param>
        /// <param name="streamName">Stream name to push events into</param>
        /// <param name="logger">Logger.</param>
        /// <param name="startingPosition">Starting Position.</param>
        /// <param name="catchupPageSize">Catchup page size.</param>
        /// <param name="upperQueueBound">Upper Queue Bound</param>
        /// <param name="heartbeatFrequency">Frequency of heartbeat.</param>
        /// <param name="heartbeatTimeout">Timeout of heartbeat</param>
        /// <param name="maxLiveQueueSize">Maximum size of the live queue</param>
        /// <returns>A new EventStoreSubscriber object.</returns>
        // ReSharper disable once UnusedMember.Global
        public static EventStoreSubscriber CreateCatchupSubscriptionFromPosition(
            IEventStoreConnection connection,
            IDispatcher<ResolvedEvent> dispatcher,
            string streamName,
            ILogger logger,
            int? startingPosition,
            int catchupPageSize = 1024,
            int upperQueueBound = 2048,
            TimeSpan? heartbeatFrequency = null,
            TimeSpan? heartbeatTimeout = null,
            int maxLiveQueueSize = 10000)
            => new EventStoreSubscriber(connection, dispatcher, streamName, logger, startingPosition, catchupPageSize, upperQueueBound, heartbeatFrequency, heartbeatTimeout, maxLiveQueueSize);
#pragma warning restore CS0618 // Type or member is obsolete

        /// <summary>
        /// Sends an event to the heartbeat stream
        /// </summary>
        public void SendHeartbeat()
            => _connection.AppendToStreamAsync(_heartbeatStreamName, ExpectedVersion.Any, new EventData(Guid.NewGuid(), HeartbeatEventType, false, new byte[0], new byte[0])).Wait();

        /// <summary>
        /// Start the subscriber.
        /// </summary>
        /// <param name="restart">Starting from a restart.</param>
        public void Start(bool restart = false)
        {
            if (_usingHeartbeats)
            {
                SendHeartbeat();
                _heartbeatTimer.Start();
            }

            lock (_subscriptionLock)
            {
                KillSubscription();

                if (_liveOnly)
                {
                    _subscription = _connection.SubscribeToStreamAsync(_streamName, true, (s, e) => EventAppeared(e), SubscriptionDropped).Result;
                }
                else
                {
                    var catchUpSettings = new CatchUpSubscriptionSettings(_maxLiveQueueSize, _catchupPageSize, true, true);
                    _subscription = _connection.SubscribeToStreamFrom(_streamName, _startingPosition, catchUpSettings, (s, e) => EventAppeared(e), LiveProcessingStarted, SubscriptionDropped);
                }
            }

            if (restart)
            {
                return;
            }

            if (_usingHeartbeats)
            {
                _connection.SetStreamMetadataAsync(_heartbeatStreamName, ExpectedVersion.Any, StreamMetadata.Create(2));
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
                switch (_subscription)
                {
                    case EventStoreSubscription liveSubscription:
                        liveSubscription.Close();
                        break;
                    case EventStoreCatchUpSubscription catchupSubscription:
                        catchupSubscription.Stop();
                        break;
                    default: return;
                }
            }
        }

        private void Init(
            IEventStoreConnection connection,
            IDispatcher<ResolvedEvent> dispatcher,
            string streamName,
            ILogger logger,
            TimeSpan? heartbeatFrequency,
            TimeSpan? heartbeatTimeout,
            int? startingPosition = null,
            int catchupPageSize = 1024,
            int upperQueueBound = 2048,
            int maxLiveQueueSize = 10000,
            bool liveOnly = false)
        {
            _liveProcessingTimer.Elapsed += LiveProcessingTimerOnElapsed;
            _logger = logger;
            _eventsProcessed = 0;
            _startingPosition = startingPosition;
            _lastReceivedEventNumber = startingPosition;
            _dispatcher = dispatcher;
            _streamName = streamName;
            _connection = connection;
            _catchupPageSize = catchupPageSize;
            _maxLiveQueueSize = maxLiveQueueSize;
            _liveOnly = liveOnly;

            if (heartbeatTimeout != null && heartbeatFrequency != null)
            {
                if (heartbeatFrequency > heartbeatTimeout)
                {
                    throw new ArgumentException("Heartbeat timeout must be greater than heartbeat frequency", nameof(heartbeatTimeout));
                }

                _heartbeatTimer = new Timer(heartbeatFrequency.Value.TotalMilliseconds);
                _heartbeatTimer.Elapsed += HeartbeatTimerOnElapsed;

                _heartbeatTimeout = heartbeatTimeout.Value;
                _lastHeartbeat = DateTime.UtcNow;
                _usingHeartbeats = true;
            }
            else if (heartbeatTimeout == null && heartbeatFrequency != null)
            {
                throw new ArgumentException("Heartbeat timeout must be set if heartbeat frequency is set", nameof(heartbeatTimeout));
            }
            else if (heartbeatTimeout != null)
            {
                throw new ArgumentException("Heartbeat frequency must be set if heartbeat timeout is set", nameof(heartbeatFrequency));
            }

            _queue = new BlockingCollection<ResolvedEvent>(upperQueueBound);
        }

        private void HeartbeatTimerOnElapsed(object sender, ElapsedEventArgs elapsedEventArgs)
        {
            SendHeartbeat();

            if (ViewModelsReady || _lastHeartbeat >= DateTime.UtcNow.Subtract(_heartbeatTimeout))
            {
                return;
            }

            _logger.Error($"Subscriber heartbeat timeout, last heartbeat: {_lastHeartbeat:G} restarting subscription");
            RestartSubscription();
        }

        private void RestartSubscription()
        {
            if (_usingHeartbeats)
            {
                _heartbeatTimer.Stop();
            }

            lock (_subscriptionLock)
            {
                KillSubscription();

                _startingPosition = _lastReceivedEventNumber;
                _lastNonLiveEventNumber = int.MinValue;
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

        private void LiveProcessingTimerOnElapsed(object sender, ElapsedEventArgs elapsedEventArgs) => _logger.Error("Event Store Subscription has been down for 10 minutes");

        private void ProcessEvents()
        {
            foreach (var item in _queue.GetConsumingEnumerable())
            {
                ProcessEvent(item);
                _lastDispatchedEventNumber = item.OriginalEventNumber;
            }
        }

        private void SubscriptionDropped(object eventStoreCatchUpSubscription, SubscriptionDropReason subscriptionDropReason, Exception ex)
        {
            if (ex != null)
            {
                _logger.Info(ex, "Event Store subscription dropped {0}", subscriptionDropReason.ToString());
            }
            else
            {
                _logger.Info("Event Store subscription dropped {0}", subscriptionDropReason.ToString());
            }

            if (subscriptionDropReason == SubscriptionDropReason.UserInitiated)
            {
                _logger.Info("Not attempting to restart user initiated drop. Subscription is dead.");
                return;
            }

            RestartSubscription();
        }

        private void LiveProcessingStarted(EventStoreCatchUpSubscription eventStoreCatchUpSubscription)
        {
            lock (_liveProcessingTimer)
            {
                _liveProcessingTimer.Stop();
                _catchingUp = false;
            }

            _logger.Info("Live event processing started");
        }

        private void EventAppeared(ResolvedEvent resolvedEvent)
        {
            if (resolvedEvent.Event != null && resolvedEvent.Event.EventType == HeartbeatEventType)
            {
                _lastHeartbeat = DateTime.UtcNow;
                return;
            }

            if (_catchingUp)
            {
                _lastNonLiveEventNumber = resolvedEvent.OriginalEventNumber;
            }

            _queue.Add(resolvedEvent);
            _lastReceivedEventNumber = resolvedEvent.OriginalEventNumber;
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

                _checkpoint.Write(resolvedEvent.OriginalEventNumber);
                _checkpoint.Flush();
            }
            catch (Exception ex)
            {
                _logger.Error(ex, "Error dispatching event from Event Store subscriber ({0}/{1})", resolvedEvent.Event.EventStreamId, resolvedEvent.Event.EventNumber);
            }
        }

        private void KillSubscription()
        {
            switch (_subscription)
            {
                case EventStoreSubscription liveSubscription:
                    liveSubscription.Dispose();
                    break;
                case EventStoreCatchUpSubscription catchUpSubscription:
                    catchUpSubscription.Stop();
                    break;
                case null: break;
                default: throw new InvalidOperationException("The event store subscription was invalid.");
            }

            _subscription = null;
        }
    }
}
