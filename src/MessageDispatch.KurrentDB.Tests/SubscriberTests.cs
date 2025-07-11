﻿// Copyright (c) Pharmaxo. All rights reserved.

using System.Diagnostics;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using CorshamScience.MessageDispatch.Core;
using DotNet.Testcontainers.Builders;
using KurrentDB.Client;
using Microsoft.Extensions.Logging.Abstractions;
using PharmaxoScientific.MessageDispatch.KurrentDB;
using Testcontainers.EventStoreDb;

namespace MessageDispatch.KurrentDB.Tests;

public class SubscriberTests
{
    private const string StreamName = "stream1";
    private string _connectionString;
    private KurrentDBClient _kurrentDbClient;
    private AwaitableDispatcherSpy _dispatcher;
    private KurrentDbSubscriber? _subscriber;

    [SetUp]
    public async Task Setup()
    {
        const int eventStoreHostPort = 1234;
        const string eventStoreVersion = "24.10.5";

        var eventStoreImageName = RuntimeInformation.OSArchitecture == Architecture.Arm64
            ? $"ghcr.io/eventstore/eventstore:{eventStoreVersion}-alpha-arm64v8"
            : $"eventstore/eventstore:{eventStoreVersion}-bookworm-slim";

        var eventStoreContainer = BuildEventStoreContainer(eventStoreImageName, eventStoreHostPort);
        await eventStoreContainer.StartAsync();

        var mappedHostPort = eventStoreContainer.GetMappedPublicPort(eventStoreHostPort);
        _connectionString = $"esdb://admin:changeit@localhost:{mappedHostPort}?tls=true&tlsVerifyCert=false";

        _kurrentDbClient = new KurrentDBClient(KurrentDBClientSettings.Create(_connectionString));
        _dispatcher = new AwaitableDispatcherSpy();
    }

    [TearDown]
    public async Task TearDown()
    {
        await _kurrentDbClient.DisposeAsync();
        _subscriber?.ShutDown();
    }

    [Test]
    public async Task CreateLiveSubscription_GivenNoEventsInStreamWhenNewEventsAdded_DispatchesEventsAndBecomesLive()
    {
        _subscriber = KurrentDbSubscriber.CreateLiveSubscription(
            _kurrentDbClient,
            _dispatcher,
            StreamName,
            new NullLogger<KurrentDbSubscriber>());

        _subscriber.Start();

        var event1 = SimpleEvent.Create();
        var event2 = SimpleEvent.Create();
        var event3 = SimpleEvent.Create();

        List<SimpleEvent> events = [event1, event2, event3];

        await AppendEventsToStreamAsync(event1, event2, event3);
        await _dispatcher.WaitForEventsToBeDispatched(event1, event2, event3);

        var deserializedDispatchedEvents =
            _dispatcher.DispatchedEvents.Select(DeserializeEventData<SimpleEvent>);

        Assert.Multiple(() =>
        {
            Assert.That(deserializedDispatchedEvents, Is.EqualTo(events));
            Assert.That(_subscriber.IsLive);
        });
    }

    [Test]
    public async Task CreateLiveSubscription_GivenExistingEventsInStreamWhenNewEventsAdded_DispatchesNewEventsAndBecomesLive()
    {
        _subscriber = KurrentDbSubscriber.CreateLiveSubscription(
            _kurrentDbClient,
            _dispatcher,
            StreamName,
            new NullLogger<KurrentDbSubscriber>());

        var oldEvent1 = SimpleEvent.Create();
        var oldEvent2 = SimpleEvent.Create();

        await AppendEventsToStreamAsync(oldEvent1, oldEvent2);

        _subscriber.Start();

        var event1 = SimpleEvent.Create();
        var event2 = SimpleEvent.Create();
        var event3 = SimpleEvent.Create();

        List<SimpleEvent> events = [event1, event2, event3];

        await AppendEventsToStreamAsync(event1, event2, event3);
        await _dispatcher.WaitForEventsToBeDispatched(event1, event2, event3);

        var deserializedDispatchedEvents =
            _dispatcher.DispatchedEvents.Select(DeserializeEventData<SimpleEvent>);

        Assert.Multiple(() =>
        {
            Assert.That(deserializedDispatchedEvents, Is.EqualTo(events));
            Assert.That(_subscriber.IsLive);
        });
    }

    [Test]
    public async Task CreateCatchupSubscriptionSubscribedToAll_GivenEventsInStream_DispatchesEventsAndBecomesLive()
    {
        _subscriber = KurrentDbSubscriber.CreateCatchupSubscriptionSubscribedToAll(
            _kurrentDbClient,
            _dispatcher,
            new NullLogger<KurrentDbSubscriber>());

        var event1 = SimpleEvent.Create();
        var event2 = SimpleEvent.Create();
        var event3 = SimpleEvent.Create();

        List<SimpleEvent> events = [event1, event2, event3];

        await AppendEventsToStreamAsync(event1, event2, event3);

        _subscriber.Start();

        await _dispatcher.WaitForEventsToBeDispatched(event1, event2, event3);

        var deserializedDispatchedEvents =
            _dispatcher.DispatchedEvents.Select(DeserializeEventData<SimpleEvent>);

        Assert.Multiple(() =>
        {
            Assert.That(deserializedDispatchedEvents, Is.EqualTo(events));
            Assert.That(_subscriber.IsLive);
        });
    }

    [Test]
    public async Task CreateCatchupSubscriptionSubscribedToAll_GivenNoEventsInStreamGivenNewEvents_DispatchesEventsAndBecomesLive()
    {
        _subscriber = KurrentDbSubscriber.CreateCatchupSubscriptionSubscribedToAll(
            _kurrentDbClient,
            _dispatcher,
            new NullLogger<KurrentDbSubscriber>());

        var event1 = SimpleEvent.Create();
        var event2 = SimpleEvent.Create();
        var event3 = SimpleEvent.Create();

        List<SimpleEvent> events = [event1, event2, event3];

        _subscriber.Start();

        await AppendEventsToStreamAsync(event1, event2, event3);
        await _dispatcher.WaitForEventsToBeDispatched(event1, event2, event3);

        var deserializedDispatchedEvents =
            _dispatcher.DispatchedEvents.Select(DeserializeEventData<SimpleEvent>);

        Assert.Multiple(() =>
        {
            Assert.That(deserializedDispatchedEvents, Is.EqualTo(events));
            Assert.That(_subscriber.IsLive);
        });
    }

    [Test]
    public async Task CreateCatchupSubscriptionFromPosition_GivenSubscribeFromStart_DispatchesEventsFromStartAndBecomesLive()
    {
        _subscriber = KurrentDbSubscriber.CreateCatchupSubscriptionFromPosition(
            _kurrentDbClient,
            _dispatcher,
            StreamName,
            new NullLogger<KurrentDbSubscriber>(),
            null);

        var event1 = SimpleEvent.Create();
        var event2 = SimpleEvent.Create();
        var event3 = SimpleEvent.Create();

        List<SimpleEvent> events = [event1, event2, event3];

        _subscriber.Start();

        await AppendEventsToStreamAsync(event1, event2, event3);

        await _dispatcher.WaitForEventsToBeDispatched(event1, event2, event3);

        var deserializedDispatchedEvents =
            _dispatcher.DispatchedEvents.Select(DeserializeEventData<SimpleEvent>);

        Assert.Multiple(() =>
        {
            Assert.That(deserializedDispatchedEvents, Is.EqualTo(events));
            Assert.That(_subscriber.IsLive);
        });
    }

    [Test]
    public async Task CreateCatchupSubscriptionFromPosition_GivenEventsInStreamAndStartPosition_DispatchesEventsFromPositionAndBecomesLive()
    {
        _subscriber = KurrentDbSubscriber.CreateCatchupSubscriptionFromPosition(
            _kurrentDbClient,
            _dispatcher,
            StreamName,
            new NullLogger<KurrentDbSubscriber>(),
            1);

        var event1 = SimpleEvent.Create();
        var event2 = SimpleEvent.Create();
        var event3 = SimpleEvent.Create();

        List<SimpleEvent> events = [event3];

        await AppendEventsToStreamAsync(event1, event2, event3);

        _subscriber.Start();

        await _dispatcher.WaitForEventsToBeDispatched(event3);

        var deserializedDispatchedEvents =
            _dispatcher.DispatchedEvents.Select(DeserializeEventData<SimpleEvent>);

        Assert.Multiple(() =>
        {
            Assert.That(deserializedDispatchedEvents, Is.EqualTo(events));
            Assert.That(_subscriber.IsLive);
        });
    }

    [Test]
    public async Task CreateCatchupSubscriptionFromPosition_GivenSubscriptionDropped_ResubscribesFromPeviousProcessedEventPosition()
    {
        _subscriber = KurrentDbSubscriber.CreateCatchupSubscriptionFromPosition(
            _kurrentDbClient,
            _dispatcher,
            StreamName,
            new NullLogger<KurrentDbSubscriber>(),
            null);

        var event1 = SimpleEvent.Create();
        var event2 = SimpleEvent.Create();
        var event3 = SimpleEvent.Create();
        var event4 = SimpleEvent.Create();
        var event5 = SimpleEvent.Create();

        _subscriber.Start();

        await AppendEventsToStreamAsync(event1, event2, event3);
        await _dispatcher.WaitForEventsToBeDispatched(event1, event2, event3);
        _dispatcher.ClearDispatchedEvents();

        // Force the client to throw an exception so that a new subscription gets created
        await _kurrentDbClient.DisposeAsync();
        await AppendEventsToStreamAsync(event4, event5);

        InitialiseSubscriberClient();

        List<SimpleEvent> postResubscribeEvents = [event4, event5];
        await _dispatcher.WaitForEventsToBeDispatched(event4, event5);

        var deserializedDispatchedEvents =
            _dispatcher.DispatchedEvents.Select(DeserializeEventData<SimpleEvent>);

        Assert.Multiple(() =>
        {
            Assert.That(deserializedDispatchedEvents, Is.EqualTo(postResubscribeEvents));
            Assert.That(_subscriber.IsLive);
        });
    }

    [Test]
    public async Task CreateCatchupSubscriptionSubscribedToAllFromPosition_GivenEventsInStreamAndStartPosition_DispatchesEventsFromPositionAndBecomesLive()
    {
        var event1 = SimpleEvent.Create();
        var event2 = SimpleEvent.Create();
        var event3 = SimpleEvent.Create();

        List<SimpleEvent> eventsExpectedToBeDispatched = [event3];

        await AppendEventsToStreamAsync(event1);
        var startingPosition = await AppendEventsToStreamAsync(event2);
        await AppendEventsToStreamAsync(event3);

        _subscriber = KurrentDbSubscriber.CreateCatchupSubscriptionSubscribedToAllFromPosition(
            _kurrentDbClient,
            _dispatcher,
            new NullLogger<KurrentDbSubscriber>(),
            startingPosition.LogPosition.CommitPosition);

        _subscriber.Start();

        await _dispatcher.WaitForEventsToBeDispatched(event3);

        var deserializedDispatchedEvents =
            _dispatcher.DispatchedEvents.Select(DeserializeEventData<SimpleEvent>);

        Assert.Multiple(() =>
        {
            Assert.That(deserializedDispatchedEvents, Is.EqualTo(eventsExpectedToBeDispatched));
            Assert.That(_subscriber.IsLive);
        });
    }

    [Test]
    public async Task CreateCatchupSubscriptionSubscribedToAllUsingCheckpoint_GivenEventsInStreamAndNoExistingCheckpointFile_DispatchesAllEventsAndBecomesLive()
    {
        var event1 = SimpleEvent.Create();
        var event2 = SimpleEvent.Create();
        var event3 = SimpleEvent.Create();

        List<SimpleEvent> eventsExpectedToBeDispatched = [event1, event2, event3];

        await AppendEventsToStreamAsync(event1);
        await AppendEventsToStreamAsync(event2);
        await AppendEventsToStreamAsync(event3);

        var checkpointFileName = Path.GetRandomFileName();

        _subscriber = KurrentDbSubscriber.CreateCatchupSubscriptionSubscribedToAllUsingCheckpoint(
            _kurrentDbClient,
            _dispatcher,
            new NullLogger<KurrentDbSubscriber>(),
            checkpointFileName);

        _subscriber.Start();

        await _dispatcher.WaitForEventsToBeDispatched(event1, event2, event3);

        var deserializedDispatchedEvents =
            _dispatcher.DispatchedEvents.Select(DeserializeEventData<SimpleEvent>);

        Assert.Multiple(() =>
        {
            Assert.That(deserializedDispatchedEvents, Is.EqualTo(eventsExpectedToBeDispatched));
            Assert.That(_subscriber.IsLive);
        });
    }

    [Test]
    public async Task CreateCatchupSubscriptionSubscribedToAllUsingCheckpoint_GivenEventsInStreamAndExistingCheckpointFile_DispatchesEventsFromPositionAndBecomesLive()
    {
        var event1 = SimpleEvent.Create();
        var event2 = SimpleEvent.Create();
        var event3 = SimpleEvent.Create();

        List<SimpleEvent> eventsExpectedToBeDispatched = [event3];

        await AppendEventsToStreamAsync(event1);
        var startingPosition = await AppendEventsToStreamAsync(event2);
        await AppendEventsToStreamAsync(event3);

        var checkpointFileName = Path.GetRandomFileName();
        var checkpoint = new WriteThroughFileCheckpoint(checkpointFileName);
        checkpoint.Write((long)startingPosition.LogPosition.CommitPosition);

        _subscriber = KurrentDbSubscriber.CreateCatchupSubscriptionSubscribedToAllUsingCheckpoint(
            _kurrentDbClient,
            _dispatcher,
            new NullLogger<KurrentDbSubscriber>(),
            checkpointFileName);

        _subscriber.Start();

        await _dispatcher.WaitForEventsToBeDispatched(event3);

        var deserializedDispatchedEvents =
            _dispatcher.DispatchedEvents.Select(DeserializeEventData<SimpleEvent>);

        Assert.Multiple(() =>
        {
            Assert.That(deserializedDispatchedEvents, Is.EqualTo(eventsExpectedToBeDispatched));
            Assert.That(_subscriber.IsLive);
        });
    }

    [Test]
    public async Task CreateCatchupSubscriptionUsingCheckpoint_GivenEventsInStreamAndNoExistingCheckpointFile_DispatchesAllEventsAndBecomesLive()
    {
        var event1 = SimpleEvent.Create();
        var event2 = SimpleEvent.Create();
        var event3 = SimpleEvent.Create();

        List<SimpleEvent> eventsExpectedToBeDispatched = [event1, event2, event3];

        await AppendEventsToStreamAsync(event1);
        await AppendEventsToStreamAsync(event2);
        await AppendEventsToStreamAsync(event3);

        var checkpointFileName = Path.GetRandomFileName();

        _subscriber = KurrentDbSubscriber.CreateCatchupSubscriptionUsingCheckpoint(
            _kurrentDbClient,
            _dispatcher,
            StreamName,
            new NullLogger<KurrentDbSubscriber>(),
            checkpointFileName);

        _subscriber.Start();

        await _dispatcher.WaitForEventsToBeDispatched(event1, event2, event3);

        var deserializedDispatchedEvents =
            _dispatcher.DispatchedEvents.Select(DeserializeEventData<SimpleEvent>);

        Assert.Multiple(() =>
        {
            Assert.That(deserializedDispatchedEvents, Is.EqualTo(eventsExpectedToBeDispatched));
            Assert.That(_subscriber.IsLive);
        });
    }

    [Test]
    public async Task CreateCatchupSubscriptionUsingCheckpoint_GivenEventsInStreamAndExistingCheckpointFile_DispatchesEventsFromPositionAndBecomesLive()
    {
        var event1 = SimpleEvent.Create();
        var event2 = SimpleEvent.Create();
        var event3 = SimpleEvent.Create();

        List<SimpleEvent> eventsExpectedToBeDispatched = [event3];

        await AppendEventsToStreamAsync(event1);
        await AppendEventsToStreamAsync(event2);
        await AppendEventsToStreamAsync(event3);

        var checkpointFileName = Path.GetRandomFileName();

        var checkpoint = new WriteThroughFileCheckpoint(checkpointFileName);
        checkpoint.Write(1);

        _subscriber = KurrentDbSubscriber.CreateCatchupSubscriptionUsingCheckpoint(
            _kurrentDbClient,
            _dispatcher,
            StreamName,
            new NullLogger<KurrentDbSubscriber>(),
            checkpointFileName);

        _subscriber.Start();

        await _dispatcher.WaitForEventsToBeDispatched(event3);

        var deserializedDispatchedEvents =
            _dispatcher.DispatchedEvents.Select(DeserializeEventData<SimpleEvent>);

        Assert.Multiple(() =>
        {
            Assert.That(deserializedDispatchedEvents, Is.EqualTo(eventsExpectedToBeDispatched));
            Assert.That(_subscriber.IsLive);
        });
    }

    [Test]
    public async Task IsLive_WhenCatchingUpUsingLinkedEventsGivenMissingLinkedEvent_ReturnsTrueOnceCaughtUp()
    {
        await AppendEventsToStreamAsync(SimpleEvent.Create(), SimpleEvent.Create());

        const string linkedStream = "non-system";
        var event1LinkedData = new EventData(
            Uuid.NewUuid(),
            SystemEventTypes.LinkTo,
            Encoding.UTF8.GetBytes($"0@{StreamName}")
        );

        var event2LinkedData = new EventData(
            Uuid.NewUuid(),
            SystemEventTypes.LinkTo,
            Encoding.UTF8.GetBytes($"1@{StreamName}")
        );

        var deletedLinkData = new EventData(
            Uuid.NewUuid(),
            SystemEventTypes.LinkTo,
            Encoding.UTF8.GetBytes($"2@{StreamName}")
        );

        await _kurrentDbClient.AppendToStreamAsync(
            linkedStream,
            StreamState.NoStream,
            [event1LinkedData, event2LinkedData, deletedLinkData]);

        _subscriber = KurrentDbSubscriber.CreateCatchupSubscriptionFromPosition(
            _kurrentDbClient,
            _dispatcher,
            linkedStream,
            new NullLogger<KurrentDbSubscriber>(),
            null);

        _subscriber.Start();

        var stopwatch = Stopwatch.StartNew();
        while (stopwatch.Elapsed < TimeSpan.FromSeconds(5))
        {
            if (_subscriber.IsLive)
            {
                break;
            }

            Thread.Sleep(TimeSpan.FromMilliseconds(100));
        }

        Assert.That(_subscriber.IsLive, "Subscriber was not live");
    }

    private class SimpleEvent
    {
        // ReSharper disable once UnusedAutoPropertyAccessor.Local
        // ReSharper disable once MemberCanBePrivate.Local
        public Guid Id { get; }

        // ReSharper disable once MemberCanBePrivate.Local
        public SimpleEvent(Guid id) => Id = id;

        public static SimpleEvent Create() => new(Guid.NewGuid());

        public override bool Equals(object? obj) => obj is SimpleEvent other && Id.Equals(other.Id);

        public override int GetHashCode() => Id.GetHashCode();

        public override string ToString() => Id.ToString();
    }

    private class AwaitableDispatcherSpy : IDispatcher<ResolvedEvent>
    {
        public List<ResolvedEvent> DispatchedEvents { get; } = [];

        public void ClearDispatchedEvents() => DispatchedEvents.Clear();

        public void Dispatch(ResolvedEvent message) => DispatchedEvents.Add(message);

        public Task WaitForEventsToBeDispatched(params object[] events)
        {
            if (events.Length == 0)
            {
                return Task.CompletedTask;
            }

            var iterations = 0;
            while (DispatchedEvents.Count != events.Length)
            {
                Thread.Sleep(50);
                iterations++;

                if (iterations > 100)
                {
                    throw new TimeoutException("Expected events weren't dispatched within the allotted time.");
                }
            }

            return Task.CompletedTask;
        }
    }

    private static T? DeserializeEventData<T>(ResolvedEvent message) =>
        JsonSerializer.Deserialize<T>(Encoding.UTF8.GetString(message.Event.Data.Span.ToArray()));

    private static EventStoreDbContainer BuildEventStoreContainer(string imageName, int hostPort) =>
        new EventStoreDbBuilder()
            .WithImage(imageName)
            .WithCleanUp(true)
            .WithCreateParameterModifier(cmd => cmd.User = "root")
            .WithPortBinding(hostPort, true)
            .WithEnvironment(new Dictionary<string, string>
            {
                { "EVENTSTORE_DEV", "true" },
                { "EVENTSTORE_INSECURE", "false" },
                { "EVENTSTORE_ENABLE_ATOM_PUB_OVER_HTTP", "true" },
                { "EVENTSTORE_HTTP_PORT", hostPort.ToString() },
                { "EVENTSTORE_RUN_PROJECTIONS", "All" },
            })
            .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(hostPort))
            .Build();

    private static EventData ToEventData(object data, JsonSerializerOptions? options = null)
    {
        var metaData = new { ClrType = data.GetType().AssemblyQualifiedName, };

        var type = data.GetType().Name;

        return new EventData(
            Uuid.NewUuid(),
            type,
            Encoding.UTF8.GetBytes(JsonSerializer.Serialize(data, options)),
            Encoding.UTF8.GetBytes(JsonSerializer.Serialize(metaData, options)));
    }

    private async Task<IWriteResult> AppendEventsToStreamAsync(params object[] events)
    {
        var eventData = events.Select(e => ToEventData(e));
        var client = new KurrentDBClient(KurrentDBClientSettings.Create(_connectionString));

        return await client.AppendToStreamAsync(StreamName, StreamState.Any, eventData);
    }

    private void InitialiseSubscriberClient()
    {
        var field = typeof(KurrentDbSubscriber)
            .GetField("_kurrentDbClient", BindingFlags.Instance | BindingFlags.NonPublic);

        _kurrentDbClient = new KurrentDBClient(KurrentDBClientSettings.Create(_connectionString));

        field?.SetValue(_subscriber, _kurrentDbClient);
    }
}
