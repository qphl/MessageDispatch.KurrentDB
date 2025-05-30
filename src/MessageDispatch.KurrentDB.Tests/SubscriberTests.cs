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

    [SetUp]
    public async Task Setup()
    {
        const int eventStoreHostPort = 1234;
        const string eventStoreVersion = "23.10.0";

        var eventStoreImageName = RuntimeInformation.OSArchitecture == Architecture.Arm64
            ? $"ghcr.io/eventstore/eventstore:{eventStoreVersion}-alpha-arm64v8"
            : $"eventstore/eventstore:{eventStoreVersion}-bookworm-slim";

        var eventStoreContainer = BuildEventStoreContainer(eventStoreImageName, eventStoreHostPort);
        await eventStoreContainer.StartAsync();

        var mappedHostPort = eventStoreContainer.GetMappedPublicPort(eventStoreHostPort);
        _connectionString = $"esdb://admin:changeit@localhost:{mappedHostPort}?tls=false";
    }

    [Test]
    public async Task CreateLiveSubscription_GivenNoEventsInStreamWhenNewEventsAdded_DispatchesEventsAndBecomesLive()
    {
        var kurrentDbClient = new KurrentDBClient(KurrentDBClientSettings.Create(_connectionString));

        var dispatcher = new AwaitableDispatcherSpy();
        var subscriber = KurrentDbSubscriber.CreateLiveSubscription(
            kurrentDbClient,
            dispatcher,
            StreamName,
            new NullLogger<KurrentDbSubscriber>());

        subscriber.Start();

        var event1 = SimpleEvent.Create();
        var event2 = SimpleEvent.Create();
        var event3 = SimpleEvent.Create();

        List<SimpleEvent> events = [event1, event2, event3];

        await AppendEventsToStreamAsync(event1, event2, event3);
        await dispatcher.WaitForEventsToBeDispatched(event1, event2, event3);

        var deserializedDispatchedEvents =
            dispatcher.DispatchedEvents.Select(DeserializeEventData<SimpleEvent>);

        Assert.Multiple(() =>
        {
            Assert.That(deserializedDispatchedEvents, Is.EqualTo(events));
            Assert.That(subscriber.IsLive);
        });
    }

    // ReSharper disable once NotAccessedPositionalProperty.Local
    private record SimpleEvent(Guid Id)
    {
        public static SimpleEvent Create() => new(Guid.NewGuid());
    }

    private class AwaitableDispatcherSpy : IDispatcher<ResolvedEvent>
    {
        public List<ResolvedEvent> DispatchedEvents { get; } = [];

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
                Thread.Sleep(100);
                iterations++;

                if (iterations > 10)
                {
                    throw new TimeoutException("Expected events weren't dispatched within the allotted time.");
                }
            }

            return Task.CompletedTask;
        }
    }

    private static T? DeserializeEventData<T>(ResolvedEvent message) =>
        JsonSerializer.Deserialize<T>(Encoding.UTF8.GetString(message.Event.Data.Span));

    private static EventStoreDbContainer BuildEventStoreContainer(string imageName, int hostPort) =>
        new EventStoreDbBuilder()
            .WithImage(imageName)
            .WithCleanUp(true)
            .WithPortBinding(hostPort, true)
            .WithEnvironment(new Dictionary<string, string>
            {
                { "EVENTSTORE_INSECURE", "true" },
                { "EVENTSTORE_ENABLE_ATOM_PUB_OVER_HTTP", "true" },
                { "EVENTSTORE_ENABLE_EXTERNAL_TCP", "true" },
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

    private async Task AppendEventsToStreamAsync(params object[] events)
    {
        var eventData = events.Select(e => ToEventData(e));
        var client = new KurrentDBClient(KurrentDBClientSettings.Create(_connectionString));

        await client.AppendToStreamAsync(StreamName, StreamState.Any, eventData);
    }
}
