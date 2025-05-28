// Copyright (c) Pharmaxo. All rights reserved.

using System;
using System.Text;
using CorshamScience.MessageDispatch.Core;
using KurrentDB.Client;
using Newtonsoft.Json.Linq;

namespace PharmaxoScientific.MessageDispatch.EventStore;

/// <inheritdoc />
/// <summary>
/// A message dispatcher that deserializes messages to a JObject upon dispatch.
/// </summary>
// ReSharper disable once UnusedMember.Global
public class KurrentDBJObjectDispatcher : DeserializingMessageDispatcher<ResolvedEvent, string>
{
#pragma warning disable SA1648 // inheritdoc should be used with inheriting class
    /// <inheritdoc />
    /// <summary>
    /// Initializes a new instance of the <see cref="KurrentDBJObjectDispatcher" /> class.
    /// </summary>
    /// <param name="handlers">Lookups for the handlers which the class can use to process messages.</param>
    // ReSharper disable once UnusedMember.Global
    public KurrentDBJObjectDispatcher(IMessageHandlerLookup<string> handlers)
        : base(handlers)
    {
    }
#pragma warning restore SA1648 // inheritdoc should be used with inheriting class

    /// <inheritdoc />
    protected override bool TryGetMessageType(ResolvedEvent rawMessage, out string type)
    {
        type = rawMessage.Event.EventType;
        return true;
    }

    /// <inheritdoc />
    protected override bool TryDeserialize(string messageType, ResolvedEvent rawMessage, out object deserialized)
    {
        deserialized = null;

        try
        {
            deserialized = JObject.Parse(Encoding.UTF8.GetString(rawMessage.Event.Data.Span.ToArray()));
            return true;
        }
        catch (Exception)
        {
            return false;
        }
    }
}
