// Copyright (c) Pharmaxo. All rights reserved.

using System;
using System.Collections.Generic;
using System.Text;
using CorshamScience.MessageDispatch.Core;
using KurrentDB.Client;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace PharmaxoScientific.MessageDispatch.EventStore;

/// <inheritdoc />
/// <summary>
/// A deserializing event dispatcher for events produced by CorshamScience.AggregatRepository.
/// </summary>
// ReSharper disable once UnusedMember.Global
public class KurrentAggregateEventDispatcher : DeserializingMessageDispatcher<ResolvedEvent, Type>
{
    private readonly JsonSerializerSettings _serializerSettings;

    private readonly Dictionary<string, Type> _typeCache = new Dictionary<string, Type>();
    private readonly string _metadataKey;

#pragma warning disable SA1648 // inheritdoc should be used with inheriting class
    /// <inheritdoc />
    /// <summary>
    /// Initializes a new instance of the <see cref="KurrentAggregateEventDispatcher" /> class.
    /// </summary>
    /// <param name="handlers">The handler methods for processing messages with.</param>
    /// <param name="serializerSettings">Determines the settings for the JSON serialization of events.</param>
    /// <param name="metadataKey">Optional parameter for a metadata key default is ClrType</param>
    // ReSharper disable once UnusedMember.Global
    public KurrentAggregateEventDispatcher(
        IMessageHandlerLookup<Type> handlers,
        JsonSerializerSettings serializerSettings = null,
        string metadataKey = null)
        : base(handlers)
    {
        _serializerSettings = serializerSettings ?? new JsonSerializerSettings();
        _metadataKey = metadataKey ?? "ClrType";
    }
#pragma warning restore SA1648 // inheritdoc should be used with inheriting class

    /// <inheritdoc />
    protected override bool TryGetMessageType(ResolvedEvent rawMessage, out Type type)
    {
        type = null;

        // optimization: don't even bother trying to deserialize metadata for system events
        if (rawMessage.Event.EventType.StartsWith("$") || rawMessage.Event.Metadata.Length == 0)
        {
            return false;
        }

        try
        {
            IDictionary<string, JToken> metadata = JObject.Parse(Encoding.UTF8.GetString(rawMessage.Event.Metadata.Span.ToArray()));

            if (!metadata.ContainsKey(_metadataKey))
            {
                return false;
            }

            string typeString = (string)metadata[_metadataKey];

            if (!_typeCache.TryGetValue(typeString, out var cached))
            {
                try
                {
                    cached = Type.GetType(
                        typeString,
                        (assemblyName) =>
                        {
                            assemblyName.Version = null;
                            return System.Reflection.Assembly.Load(assemblyName);
                        },
                        null,
                        true,
                        true);
                }
                catch (Exception)
                {
                    cached = typeof(TypeNotFound);
                }

                _typeCache.Add(typeString, cached);
            }

            if (cached?.Name.Equals("TypeNotFound") ?? false)
            {
                return false;
            }

            type = cached;
            return true;
        }
        catch (Exception)
        {
            return false;
        }
    }

    /// <inheritdoc />
    protected override bool TryDeserialize(Type messageType, ResolvedEvent rawMessage, out object deserialized)
    {
        deserialized = null;

        try
        {
            var jsonString = Encoding.UTF8.GetString(rawMessage.Event.Data.Span.ToArray());
            deserialized = JsonConvert.DeserializeObject(jsonString, messageType, _serializerSettings);
            return deserialized != null;
        }
        catch (Exception)
        {
            return false;
        }
    }

    private class TypeNotFound
    {
    }
}
