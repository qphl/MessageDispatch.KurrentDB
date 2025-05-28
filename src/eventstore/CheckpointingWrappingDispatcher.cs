// Copyright (c) Pharmaxo. All rights reserved.

using System;
using System.Text;
using CorshamScience.MessageDispatch.Core;
using global::EventStore.Client;
using Newtonsoft.Json.Linq;

namespace PharmaxoScientific.MessageDispatch.EventStore;

/// <summary>
/// A wrapping event dispatcher which keeps track of a checkpoint, and whether the dispatched event has been previously processed or not.
/// </summary>
public class CheckpointingWrappingDispatcher : KurrentAggregateEventDispatcher
{
    private readonly WriteThroughFileCheckpoint _checkpoint;
    private readonly long _startupCheckpointValue;

    /// <summary>
    /// Initializes a new instance of the <see cref="CheckpointingWrappingDispatcher" /> class.
    /// </summary>
    /// <param name="checkpointFilePath">The file to write a checkpoint to.</param>
    /// <param name="initValue">The initial value to write.</param>
    /// <param name="handlers">The handler methods for processing messages with.</param>
    /// <param name="metadataKey">The metadata key.</param>
    public CheckpointingWrappingDispatcher(
        string checkpointFilePath,
        long initValue,
        IMessageHandlerLookup<Type> handlers,
        string metadataKey = null)
        : base(handlers, null, metadataKey)
    {
        _checkpoint = new WriteThroughFileCheckpoint(checkpointFilePath, initValue);
        _startupCheckpointValue = _checkpoint.Read();
    }

    /// <inheritdoc />
    public override void Dispatch(ResolvedEvent message)
    {
        base.Dispatch(message);

        var previouslyProcessed = message.OriginalEventNumber.ToInt64() <= _startupCheckpointValue;

        if (previouslyProcessed)
        {
            return;
        }

        _checkpoint.Write(message.OriginalEventNumber.ToInt64());
    }

    /// <inheritdoc/>
    protected override bool TryDeserialize(
        Type messageType,
        ResolvedEvent rawMessage,
        out object deserialized)
    {
        deserialized = null!;

        var previouslyProcessed = rawMessage.OriginalEventNumber.ToInt64() <= _startupCheckpointValue;

        try
        {
            var jsonString = Encoding.UTF8.GetString(rawMessage.Event.Data.Span.ToArray());
            var @event = JObject.Parse(jsonString);

            deserialized = new EventWrapper(@event, previouslyProcessed);

            return deserialized != null;
        }
        catch (Exception)
        {
            return false;
        }
    }
}
