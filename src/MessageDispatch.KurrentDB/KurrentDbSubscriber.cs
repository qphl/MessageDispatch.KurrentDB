// Copyright (c) Pharmaxo. All rights reserved.

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CorshamScience.MessageDispatch.Core;
using KurrentDB.Client;
using Microsoft.Extensions.Logging;
using static KurrentDB.Client.KurrentDBClient;

namespace PharmaxoScientific.MessageDispatch.KurrentDB;

/// <summary>
/// Subscriber for event store.
/// </summary>
public class KurrentDbSubscriber : IKurrentDbSubscriber
{
    private const string AllStreamName = "$all";
    private const uint CheckpointInterval = 1;
    private readonly WriteThroughFileCheckpoint _checkpoint;
    private KurrentDBClient _kurrentDbClient;
    private ulong? _startingPosition;
    private string _streamName;
    private bool _liveOnly;
    private bool _subscribeToAll;
    private ulong? _lastProcessedEventPosition;
    private ulong _actualEndOfStreamPosition;
    private CancellationTokenSource _cts;
    private DateTime _lastStreamPositionTimestamp;
    private Func<Task> _setLastPositions;

    private IDispatcher<ResolvedEvent> _dispatcher;
    private ILogger _logger;

    private KurrentDbSubscriber(
        KurrentDBClient kurrentDbClient,
        IDispatcher<ResolvedEvent> dispatcher,
        string streamName,
        ILogger logger,
        ulong? startingPosition)
        => Init(kurrentDbClient, dispatcher, streamName, logger, startingPosition);

    private KurrentDbSubscriber(
        KurrentDBClient kurrentDbClient,
        IDispatcher<ResolvedEvent> dispatcher,
        ILogger logger,
        string streamName,
        string checkpointFilePath)
    {
        _checkpoint = new WriteThroughFileCheckpoint(checkpointFilePath, -1);
        var initialCheckpointPosition = _checkpoint.Read();
        ulong? startingPosition = null;

        if (initialCheckpointPosition != -1)
        {
            startingPosition = (ulong)initialCheckpointPosition;
        }

        Init(kurrentDbClient, dispatcher, streamName, logger, startingPosition);
    }

    private KurrentDbSubscriber(
        KurrentDBClient kurrentDbClient,
        IDispatcher<ResolvedEvent> dispatcher,
        string streamName,
        ILogger logger)
        => Init(kurrentDbClient, dispatcher, streamName, logger, liveOnly: true);

    /// <summary>
    /// Gets a new catchup progress object.
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public CatchupProgress CatchupProgress
    {
        get
        {
            var lastStreamPosition = GetEndOfStreamPosition().Result;

            return new CatchupProgress(
                _lastProcessedEventPosition ?? 0,
                _streamName,
                lastStreamPosition,
                _startingPosition ?? 0,
                _subscribeToAll);
        }
    }

    /// <summary>
    /// Gets a value indicating whether the view model is ready or not.
    /// </summary>
    /// <returns>Returns true if catchup is within threshold.</returns>
    public bool IsLive { get; set; } = false;

    /// <summary>
    /// Creates a live KurrentDB subscription.
    /// </summary>
    /// <param name="kurrentDbClient">KurrentDB connection.</param>
    /// <param name="dispatcher">Dispatcher.</param>
    /// <param name="streamName">Stream name to push events into.</param>
    /// <param name="logger">Logger.</param>
    /// <returns>A new KurrentDbSubscriber object.</returns>
    // ReSharper disable once UnusedMember.Global
    public static KurrentDbSubscriber CreateLiveSubscription(
        KurrentDBClient kurrentDbClient,
        IDispatcher<ResolvedEvent> dispatcher,
        string streamName,
        ILogger logger)
        => new KurrentDbSubscriber(kurrentDbClient, dispatcher, streamName, logger);

    /// <summary>
    /// Creates an KurrentDB catchup subscription using a checkpoint file.
    /// </summary>
    /// <param name="kurrentDbClient">KurrentDB connection.</param>
    /// <param name="dispatcher">Dispatcher.</param>
    /// <param name="streamName">Stream name to push events into.</param>
    /// <param name="logger">Logger.</param>
    /// <param name="checkpointFilePath">Path of the checkpoint file.</param>
    /// <returns>A new KurrentDbSubscriber object.</returns>
    // ReSharper disable once UnusedMember.Global
    public static KurrentDbSubscriber CreateCatchupSubscriptionUsingCheckpoint(
        KurrentDBClient kurrentDbClient,
        IDispatcher<ResolvedEvent> dispatcher,
        string streamName,
        ILogger logger,
        string checkpointFilePath)
        => new KurrentDbSubscriber(kurrentDbClient, dispatcher, logger, streamName, checkpointFilePath);

    /// <summary>
    /// Creates an KurrentDB catchup subscription from a position.
    /// </summary>
    /// <param name="kurrentDbClient">KurrentDB connection.</param>
    /// <param name="dispatcher">Dispatcher.</param>
    /// <param name="streamName">Stream name to push events into.</param>
    /// <param name="logger">Logger.</param>
    /// <param name="startingPosition">Starting Position.</param>
    /// <returns>A new KurrentDbSubscriber object.</returns>
    // ReSharper disable once UnusedMember.Global
    public static KurrentDbSubscriber CreateCatchupSubscriptionFromPosition(
        KurrentDBClient kurrentDbClient,
        IDispatcher<ResolvedEvent> dispatcher,
        string streamName,
        ILogger logger,
        ulong? startingPosition)
        => new KurrentDbSubscriber(kurrentDbClient, dispatcher, streamName, logger, startingPosition);

    /// <summary>
    /// Creates an KurrentDB catchup subscription that is subscribed to all from the start.
    /// </summary>
    /// <param name="kurrentDbClient">KurrentDB connection.</param>
    /// <param name="dispatcher">Dispatcher.</param>
    /// <param name="logger">Logger.</param>
    /// <returns>A new KurrentDbSubscriber object.</returns>
    // ReSharper disable once UnusedMember.Global
    public static KurrentDbSubscriber CreateCatchupSubscriptionSubscribedToAll(
        KurrentDBClient kurrentDbClient,
        IDispatcher<ResolvedEvent> dispatcher,
        ILogger logger)
        => new KurrentDbSubscriber(
            kurrentDbClient,
            dispatcher,
            AllStreamName,
            logger);

    /// <summary>
    /// Creates an KurrentDB catchup subscription that is subscribed to all from a position.
    /// </summary>
    /// <param name="kurrentDbClient">KurrentDB connection.</param>
    /// <param name="dispatcher">Dispatcher.</param>
    /// <param name="logger">Logger.</param>
    /// <param name="startingPosition">Starting Position.</param>
    /// <returns>A new KurrentDbSubscriber object.</returns>
    // ReSharper disable once UnusedMember.Global
    public static KurrentDbSubscriber CreateCatchupSubscriptionSubscribedToAllFromPosition(
        KurrentDBClient kurrentDbClient,
        IDispatcher<ResolvedEvent> dispatcher,
        ILogger logger,
        ulong? startingPosition)
        => new KurrentDbSubscriber(
            kurrentDbClient,
            dispatcher,
            AllStreamName,
            logger,
            startingPosition);

    /// <summary>
    /// Creates an KurrentDB catchup subscription subscribed to all using a checkpoint file.
    /// </summary>
    /// <param name="kurrentDbClient">KurrentDB connection.</param>
    /// <param name="dispatcher">Dispatcher.</param>
    /// <param name="logger">Logger.</param>
    /// <param name="checkpointFilePath">Path of the checkpoint file.</param>
    /// <returns>A new KurrentDbSubscriber object.</returns>
    // ReSharper disable once UnusedMember.Global
    public static KurrentDbSubscriber CreateCatchupSubscriptionSubscribedToAllUsingCheckpoint(
        KurrentDBClient kurrentDbClient,
        IDispatcher<ResolvedEvent> dispatcher,
        ILogger logger,
        string checkpointFilePath)
        => new KurrentDbSubscriber(
                kurrentDbClient,
                dispatcher,
                logger,
                AllStreamName,
                checkpointFilePath);

    /// <summary>
    /// Start the subscriber.
    /// </summary>
    // ReSharper disable once MemberCanBePrivate.Global
    public async void Start()
    {
        _cts = new CancellationTokenSource();

        while (true)
        {
            try
            {
                var subscription = CreateSubscription();
                _logger.LogInformation("Subscribed to '{StreamName}'", _streamName);

                await foreach (var message in subscription.Messages)
                {
                    switch (message)
                    {
                        case StreamMessage.Event(var @event):
                            ProcessEvent(@event);

                            var lastProcessedEventPosition = GetLastProcessedPosition(@event);

                            if (_liveOnly && _lastProcessedEventPosition is null)
                            {
                                _startingPosition = lastProcessedEventPosition;
                            }

                            _lastProcessedEventPosition = lastProcessedEventPosition;
                            break;
                        case StreamMessage.AllStreamCheckpointReached(var allPosition):
                            _lastProcessedEventPosition = allPosition.CommitPosition;
                            WriteCheckpoint((ulong)_lastProcessedEventPosition);
                            break;
                        case StreamMessage.CaughtUp:
                            _logger.LogInformation("Stream caught up: {0}", _streamName);
                            IsLive = true;
                            break;
                        case StreamMessage.FellBehind:
                            _logger.LogWarning("Stream falling behind: {0}", _streamName);
                            IsLive = false;
                            break;
                    }
                }
            }
            // User initiated drop, do not resubscribe
            catch (OperationCanceledException ex)
            {
                IsLive = false;
                _logger.LogInformation(ex, "Event Store subscription dropped {0}", SubscriptionDroppedReason.Disposed);
                break;
            }
            // User initiated drop, do not resubscribe
            catch (ObjectDisposedException ex)
            {
                IsLive = false;
                _logger.LogInformation(ex, "Event Store subscription dropped {0}", SubscriptionDroppedReason.Disposed);
                break;
            }
            catch (Exception ex)
            {
                IsLive = false;
                _logger.LogError(ex, "Event Store subscription dropped {0}", SubscriptionDroppedReason.SubscriberError);
                Console.WriteLine(ex);
            }

            // Sleep between reconnections to not flood the database or not kill the CPU with infinite loop
            // Randomness added to reduce the chance of multiple subscriptions trying to reconnect at the same time
            await Task.Delay(1000 + new Random((int)DateTime.UtcNow.Ticks).Next(1000));
        }
    }

    private StreamSubscriptionResult CreateSubscription()
    {
        var filterOptions = new SubscriptionFilterOptions(EventTypeFilter.ExcludeSystemEvents(), CheckpointInterval);

        const bool resolveLinkTos = true;

        if (_subscribeToAll)
        {
            var subscriptionStart = FromAll.End;
            if (!_liveOnly)
            {
                subscriptionStart = _startingPosition.HasValue ? FromAll.After(new Position(_startingPosition.Value, _startingPosition.Value)) : FromAll.Start;
            }

            return _kurrentDbClient.SubscribeToAll(subscriptionStart, resolveLinkTos, filterOptions, cancellationToken: _cts.Token);
        }
        else
        {
            var subscriptionStart = FromStream.End;
            if (!_liveOnly)
            {
                subscriptionStart = _startingPosition.HasValue ? FromStream.After(new StreamPosition(_startingPosition.Value)) : FromStream.Start;
            }

            return _kurrentDbClient.SubscribeToStream(_streamName, FromStream.End, resolveLinkTos, cancellationToken: _cts.Token);
        }
    }

    /// <summary>
    /// Shut down the subscription.
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public void ShutDown()
    {
        _cts.Cancel();
    }

    private void Init(
        KurrentDBClient connection,
        IDispatcher<ResolvedEvent> dispatcher,
        string streamName,
        ILogger logger,
        ulong? startingPosition = null,
        bool liveOnly = false)
    {
        _logger = logger;
        _startingPosition = startingPosition;
        _lastProcessedEventPosition = startingPosition;
        _dispatcher = dispatcher;
        _streamName = streamName;
        _kurrentDbClient = connection;
        _liveOnly = liveOnly;
        _subscribeToAll = streamName == AllStreamName;
        _lastStreamPositionTimestamp = DateTime.MinValue;

        _setLastPositions = _subscribeToAll
            ? async () =>
            {
                var eventsWithinThreshold = await _kurrentDbClient.ReadAllAsync(
                        Direction.Backwards,
                        Position.End,
                        maxCount: 1,
                        resolveLinkTos: false)
                    .ToListAsync();

                _actualEndOfStreamPosition = eventsWithinThreshold.First().OriginalEvent.Position.CommitPosition;
            }
        : async () =>
        {
            var eventsWithinThreshold = await _kurrentDbClient.ReadStreamAsync(
                    Direction.Backwards,
                    _streamName,
                    StreamPosition.End,
                    maxCount: 1,
                    resolveLinkTos: false)
                .ToListAsync();

            _actualEndOfStreamPosition = eventsWithinThreshold.First().OriginalEventNumber.ToUInt64();
        };
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
            _logger.LogTrace(
                "Event dispatched from subscriber ({0}/{1})",
                resolvedEvent.Event.EventStreamId,
                resolvedEvent.Event.EventNumber);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Error dispatching event from subscriber ({0}/{1})",
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
        _logger.LogTrace("Checkpoint written. Checkpoint number {CheckpointNumber}", checkpointNumber);
    }

    private async Task<ulong> GetEndOfStreamPosition()
    {
        var streamPositionIsStale = (DateTime.UtcNow - _lastStreamPositionTimestamp) > TimeSpan.FromSeconds(10);

        if (!_cts.Token.IsCancellationRequested && streamPositionIsStale)
        {
            await _setLastPositions();
            _lastStreamPositionTimestamp = DateTime.UtcNow;
        }

        return _actualEndOfStreamPosition;
    }
}
