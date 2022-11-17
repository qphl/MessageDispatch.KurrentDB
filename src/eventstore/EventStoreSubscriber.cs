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

        private ulong _lastNonLiveEventNumber = StreamPosition.Start;
        private ulong? _lastProcessedEventNumber;
        private int _eventsProcessed;
        private bool _catchingUp = true;

        private IDispatcher<ResolvedEvent> _dispatcher;
        private ILogger _logger;

        private EventStoreSubscriber(
            EventStoreClient eventStoreClient,
            IDispatcher<ResolvedEvent> dispatcher,
            string streamName,
            ILogger logger,
            ulong? startingPosition)
            => Init(eventStoreClient, dispatcher, streamName, logger, startingPosition);

        private EventStoreSubscriber(
            EventStoreClient eventStoreClient,
            IDispatcher<ResolvedEvent> dispatcher,
            ILogger logger,
            string streamName,
            string checkpointFilePath)
        {
            _checkpoint = new WriteThroughFileCheckpoint(checkpointFilePath, "lastProcessedPosition", false, StreamPosition.Start);
            var initialCheckpointPosition = _checkpoint.Read();
            ulong? startingPosition = null;

            if (initialCheckpointPosition != StreamPosition.Start)
            {
                startingPosition = initialCheckpointPosition;
            }

            Init(eventStoreClient, dispatcher, streamName, logger, startingPosition);
        }

        private EventStoreSubscriber(
            EventStoreClient eventStoreClient,
            IDispatcher<ResolvedEvent> dispatcher,
            string streamName,
            ILogger logger)
            => Init(eventStoreClient, dispatcher, streamName, logger, liveOnly: true);

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
        public bool ViewModelsReady => !_catchingUp && _lastProcessedEventNumber >= _lastNonLiveEventNumber;

        /// <summary>
        /// Creates a live eventstore subscription.
        /// </summary>
        /// <param name="eventStoreClient">Eventstore connection.</param>
        /// <param name="dispatcher">Dispatcher.</param>
        /// <param name="streamName">Stream name to push events into.</param>
        /// <param name="logger">Logger.</param>
        /// <returns>A new EventStoreSubscriber object.</returns>
        // ReSharper disable once UnusedMember.Global
        public static EventStoreSubscriber CreateLiveSubscription(
            EventStoreClient eventStoreClient,
            IDispatcher<ResolvedEvent> dispatcher,
            string streamName,
            ILogger logger)
            => new EventStoreSubscriber(eventStoreClient, dispatcher, streamName, logger);

        #pragma warning disable CS0618 // Type or member is obsolete
        /// <summary>
        /// Creates an eventstore catchup subscription using a checkpoint file.
        /// </summary>
        /// <param name="eventStoreClient">Eventstore connection.</param>
        /// <param name="dispatcher">Dispatcher.</param>
        /// <param name="streamName">Stream name to push events into.</param>
        /// <param name="logger">Logger.</param>
        /// <param name="checkpointFilePath">Path of the checkpoint file.</param>
        /// <returns>A new EventStoreSubscriber object.</returns>
        // ReSharper disable once UnusedMember.Global
        public static EventStoreSubscriber CreateCatchupSubscriptionUsingCheckpoint(
            EventStoreClient eventStoreClient,
            IDispatcher<ResolvedEvent> dispatcher,
            string streamName,
            ILogger logger,
            string checkpointFilePath)
            => new EventStoreSubscriber(eventStoreClient, dispatcher, logger, streamName, checkpointFilePath);

        /// <summary>
        /// Creates an ecventstore catchup subscription from a position.
        /// </summary>
        /// <param name="eventStoreClient">Eventstore connection.</param>
        /// <param name="dispatcher">Dispatcher.</param>
        /// <param name="streamName">Stream name to push events into.</param>
        /// <param name="logger">Logger.</param>
        /// <param name="startingPosition">Starting Position.</param>
        /// <returns>A new EventStoreSubscriber object.</returns>
        // ReSharper disable once UnusedMember.Global
        public static EventStoreSubscriber CreateCatchupSubscriptionFromPosition(
            EventStoreClient eventStoreClient,
            IDispatcher<ResolvedEvent> dispatcher,
            string streamName,
            ILogger logger,
            ulong? startingPosition)
            => new EventStoreSubscriber(eventStoreClient, dispatcher, streamName, logger, startingPosition);
#pragma warning restore CS0618 // Type or member is obsolete

        /// <summary>
        /// Start the subscriber.
        /// </summary>
        public void Start()
        {
            while (true)
            {
                var resubscribed = false;

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

                    resubscribed = true;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to resubscribe to '{StreamName}' dropped with '{ExceptionMessage}{ExceptionStackTrace}'", _streamName, ex.Message, ex.StackTrace);
                }
                finally
                {
                    Monitor.Exit(_subscriptionLock);
                }

                if (resubscribed)
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
                _subscription.Dispose();
            }
        }

        private void Init(
            EventStoreClient connection,
            IDispatcher<ResolvedEvent> dispatcher,
            string streamName,
            ILogger logger,
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
        }

        private void RestartSubscription()
        {
            _startingPosition = _lastProcessedEventNumber;
            _lastNonLiveEventNumber = StreamPosition.Start;
            _catchingUp = true;

            Start();
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
