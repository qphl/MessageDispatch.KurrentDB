// <copyright file="EventStoreSubscriber.cs" company="Corsham Science">
// Copyright (c) Corsham Science. All rights reserved.
// </copyright>

namespace CorshamScience.MessageDispatch.EventStore
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Timers;
    using CorshamScience.MessageDispatch.Core;
    using global::EventStore.Client;
    using Microsoft.Extensions.Logging;
    using static global::EventStore.Client.StreamMessage;
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

        private EventStoreClient _eventStoreClient;
        private ulong? _startingPosition;
        private int _maxLiveQueueSize;
        private StreamSubscription _subscription;
        private int _catchupPageSize;
        private string _streamName;
        private bool _liveOnly;

        private ulong _lastNonLiveEventNumber = ulong.MinValue;
        private ulong? _lastReceivedEventNumber;
        private int _eventsProcessed;
        private bool _catchingUp = true;

        private TimeSpan _heartbeatTimeout;
        private DateTime _lastHeartbeat;
        private Timer _heartbeatTimer;
        private bool _usingHeartbeats;

        private BlockingCollection<ResolvedEvent> _queue;
        private IDispatcher<ResolvedEvent> _dispatcher;
        private ulong _lastDispatchedEventNumber;

        private ILogger _logger;

        /// <summary>
        /// Initializes a new instance of the <see cref="EventStoreSubscriber"/> class.
        /// </summary>
        /// <param name="eventStoreClient">Eventstore connection.</param>
        /// <param name="dispatcher">Dispatcher.</param>
        /// <param name="streamName">Stream name to push events into.</param>
        /// <param name="logger">Logger.</param>
        /// <param name="startingPosition">Starting Position.</param>
        /// <param name="catchUpPageSize">Catchup page size.</param>
        /// <param name="upperQueueBound">Upper Queue Bound.</param>
        /// <param name="heartbeatFrequency">Frequency of heartbeat.</param>
        /// <param name="heartbeatTimeout">Timeout of heartbeat.</param>
        /// <param name="maxLiveQueueSize">Maximum size of the live queue.</param>
        [Obsolete("Please use new static method CreateCatchUpSubscirptionFromPosition, this constructor will be removed in the future.")]
        public EventStoreSubscriber(
            EventStoreClient eventStoreClient,
            IDispatcher<ResolvedEvent> dispatcher,
            string streamName,
            ILogger logger,
            int? startingPosition,
            int catchUpPageSize = 1024,
            int upperQueueBound = 2048,
            TimeSpan? heartbeatFrequency = null,
            TimeSpan? heartbeatTimeout = null,
            int maxLiveQueueSize = 10000)
            => Init(eventStoreClient, dispatcher, streamName, logger, heartbeatFrequency, heartbeatTimeout, startingPosition, catchUpPageSize, upperQueueBound, maxLiveQueueSize);

        /// <summary>
        /// Initializes a new instance of the <see cref="EventStoreSubscriber"/> class.
        /// </summary>
        /// <param name="eventStoreClient">Eventstore connection.</param>
        /// <param name="dispatcher">Dispatcher.</param>
        /// <param name="streamName">Stream name to push events into.</param>
        /// <param name="logger">Logger.</param>
        /// <param name="checkpointFilePath">Path of the checkpoint file.</param>
        /// <param name="catchupPageSize">Catchup page size.</param>
        /// <param name="upperQueueBound">Upper Queue Bound.</param>
        /// <param name="heartbeatFrequency">Frequency of heartbeat.</param>
        /// <param name="heartbeatTimeout">Timeout of heartbeat.</param>
        /// <param name="maxLiveQueueSize">Maximum size of the live queue.</param>
        [Obsolete("Please use new static method CreateCatchupSubscriptionUsingCheckpoint, this constructor will be removed in the future.")]
        public EventStoreSubscriber(
            EventStoreClient eventStoreClient,
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

            Init(eventStoreClient, dispatcher, streamName, logger, heartbeatFrequency, heartbeatTimeout, startingPosition, catchupPageSize, upperQueueBound, maxLiveQueueSize);
        }

        private EventStoreSubscriber(
            EventStoreClient eventStoreClient,
            IDispatcher<ResolvedEvent> dispatcher,
            string streamName,
            ILogger logger,
            int upperQueueBound = 2048,
            TimeSpan? heartbeatFrequency = null,
            TimeSpan? heartbeatTImeout = null)
            => Init(eventStoreClient, dispatcher, streamName, logger, heartbeatFrequency, heartbeatTImeout, upperQueueBound: upperQueueBound, liveOnly: true);

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
                    resolveLinkTos: false).LastAsync().Result.Event.EventNumber.ToUInt64();

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
        /// <param name="heartbeatFrequency">Frequency of heartbeat.</param>
        /// <param name="heartbeatTimeout">Timeout of heartbeat.</param>
        /// <returns>A new EventStoreSubscriber object.</returns>
        // ReSharper disable once UnusedMember.Global
        public static EventStoreSubscriber CreateLiveSubscription(
            EventStoreClient eventStoreClient,
            IDispatcher<ResolvedEvent> dispatcher,
            string streamName,
            ILogger logger,
            int upperQueueBound = 2048,
            TimeSpan? heartbeatFrequency = null,
            TimeSpan? heartbeatTimeout = null)
            => new EventStoreSubscriber(eventStoreClient, dispatcher, streamName, logger, upperQueueBound, heartbeatFrequency, heartbeatTimeout);

#pragma warning disable CS0618 // Type or member is obsolete
        /// <summary>
        /// Creates an eventstore catchup subscription using a checkpoint file.
        /// </summary>
        /// <param name="eventStoreClient">Eventstore connection.</param>
        /// <param name="dispatcher">Dispatcher.</param>
        /// <param name="streamName">Stream name to push events into.</param>
        /// <param name="logger">Logger.</param>
        /// <param name="checkpointFilePath">Path of the checkpoint file.</param>
        /// <param name="catchupPageSize">Catchup page size.</param>
        /// <param name="upperQueueBound">Upper Queue Bound.</param>
        /// <param name="heartbeatFrequency">Frequency of heartbeat.</param>
        /// <param name="heartbeatTimeout">Timeout of heartbeat.</param>
        /// <param name="maxLiveQueueSize">Maximum size of the live queue.</param>
        /// <returns>A new EventStoreSubscriber object.</returns>
        // ReSharper disable once UnusedMember.Global
        public static EventStoreSubscriber CreateCatchupSubscriptionUsingCheckpoint(
            EventStoreClient eventStoreClient,
            IDispatcher<ResolvedEvent> dispatcher,
            string streamName,
            ILogger logger,
            string checkpointFilePath,
            int catchupPageSize = 1024,
            int upperQueueBound = 2048,
            TimeSpan? heartbeatFrequency = null,
            TimeSpan? heartbeatTimeout = null,
            int maxLiveQueueSize = 10000)
            => new EventStoreSubscriber(eventStoreClient, dispatcher, logger, streamName, checkpointFilePath, catchupPageSize, upperQueueBound, heartbeatFrequency, heartbeatTimeout, maxLiveQueueSize);

        /// <summary>
        /// Creates an ecventstore catchup subscription from a position.
        /// </summary>
        /// <param name="eventStoreClient">Eventstore connection.</param>
        /// <param name="dispatcher">Dispatcher.</param>
        /// <param name="streamName">Stream name to push events into.</param>
        /// <param name="logger">Logger.</param>
        /// <param name="startingPosition">Starting Position.</param>
        /// <param name="catchupPageSize">Catchup page size.</param>
        /// <param name="upperQueueBound">Upper Queue Bound.</param>
        /// <param name="heartbeatFrequency">Frequency of heartbeat.</param>
        /// <param name="heartbeatTimeout">Timeout of heartbeat.</param>
        /// <param name="maxLiveQueueSize">Maximum size of the live queue.</param>
        /// <returns>A new EventStoreSubscriber object.</returns>
        // ReSharper disable once UnusedMember.Global
        public static EventStoreSubscriber CreateCatchupSubscriptionFromPosition(
            EventStoreClient eventStoreClient,
            IDispatcher<ResolvedEvent> dispatcher,
            string streamName,
            ILogger logger,
            int? startingPosition,
            int catchupPageSize = 1024,
            int upperQueueBound = 2048,
            TimeSpan? heartbeatFrequency = null,
            TimeSpan? heartbeatTimeout = null,
            int maxLiveQueueSize = 10000)
            => new EventStoreSubscriber(eventStoreClient, dispatcher, streamName, logger, startingPosition, catchupPageSize, upperQueueBound, heartbeatFrequency, heartbeatTimeout, maxLiveQueueSize);
#pragma warning restore CS0618 // Type or member is obsolete

        /// <summary>
        /// Sends an event to the heartbeat stream.
        /// </summary>
        public void SendHeartbeat()
            => _eventStoreClient.AppendToStreamAsync(_heartbeatStreamName, StreamState.Any, new List<EventData>
            {
                new EventData(
                    Uuid.NewUuid(),
                    HeartbeatEventType,
                    new byte[0],
                    new byte[0]),
            }).Wait();

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
                    _subscription = _eventStoreClient.SubscribeToStreamAsync(
                        _streamName,
                        FromStream.Start,
                        (s, e, t) => EventAppeared(e),
                        false,
                        SubscriptionDropped).Result;
                }
                else
                {
                    if (_startingPosition.HasValue)
                    {
                        var streamPosition = new StreamPosition(_startingPosition.Value);

                        _subscription = _eventStoreClient.SubscribeToStreamAsync(
                            _streamName,
                            FromStream.After(streamPosition),
                            (s, e, t) => EventAppeared(e),
                            false,
                            SubscriptionDropped).Result;
                    }
                    else
                    {
                        _subscription = _eventStoreClient.SubscribeToStreamAsync(
                            _streamName,
                            FromStream.End,
                            (s, e, t) => EventAppeared(e),
                            false,
                            SubscriptionDropped).Result;
                    }

                    var catchUpSettings = new CatchUpSubscriptionSettings(_maxLiveQueueSize, _catchupPageSize, true, true);
                    _subscription = _eventStoreClient.SubscribeToStreamFrom(_streamName, _startingPosition, catchUpSettings, (s, e) => EventAppeared(e), LiveProcessingStarted, SubscriptionDropped);
                }
            }

            if (restart)
            {
                return;
            }

            if (_usingHeartbeats)
            {
                _eventStoreClient.SetStreamMetadataAsync(_heartbeatStreamName, StreamState.Any, new StreamMetadata(2));
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
            EventStoreClient connection,
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
            _eventStoreClient = connection;
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

            _logger.LogError($"Subscriber heartbeat timeout, last heartbeat: {_lastHeartbeat:G} restarting subscription");
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
                _lastDispatchedEventNumber = item.OriginalEventNumber.ToInt64();
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

            RestartSubscription();
        }

        private void LiveProcessingStarted()
        {
            lock (_liveProcessingTimer)
            {
                _liveProcessingTimer.Stop();
                _catchingUp = false;
            }

            _logger.LogInformation("Live event processing started");
        }

        private Task EventAppeared(ResolvedEvent resolvedEvent)
        {
            if (resolvedEvent.Event != null && resolvedEvent.Event.EventType == HeartbeatEventType)
            {
                _lastHeartbeat = DateTime.UtcNow;

                return Task.CompletedTask;
            }

            if (_catchingUp)
            {
                _lastNonLiveEventNumber = resolvedEvent.OriginalEventNumber.ToInt64();
            }

            _queue.Add(resolvedEvent);
            _lastReceivedEventNumber = resolvedEvent.OriginalEventNumber.ToInt64();

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

                _checkpoint.Write(resolvedEvent.OriginalEventNumber.ToInt64());
                _checkpoint.Flush();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error dispatching event from Event Store subscriber ({0}/{1})", resolvedEvent.Event.EventStreamId, resolvedEvent.Event.EventNumber);
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
