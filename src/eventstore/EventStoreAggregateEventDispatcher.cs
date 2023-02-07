// <copyright file="EventStoreAggregateEventDispatcher.cs" company="Corsham Science">
// Copyright (c) Corsham Science. All rights reserved.
// </copyright>

namespace CorshamScience.MessageDispatch.EventStore
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using CorshamScience.MessageDispatch.Core;
    using global::EventStore.Client;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    /// <inheritdoc />
    /// <summary>
    /// A deserializing event dispatcher for events produced by CorshamScience.AggregatRepository.
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class EventStoreAggregateEventDispatcher : DeserializingMessageDispatcher<ResolvedEvent, Type>
    {
        private readonly JsonSerializerSettings _serializerSettings;
        private readonly Dictionary<string, Type> _typeCache = new Dictionary<string, Type>();
        private readonly string _metadataKey;

#pragma warning disable SA1648 // inheritdoc should be used with inheriting class
        /// <inheritdoc />
        /// <summary>
        /// Initializes a new instance of the <see cref="EventStoreAggregateEventDispatcher" /> class.
        /// </summary>
        /// <param name="handlers">The handler methods for processing messages with.</param>
        /// <param name="serializerSettings">Determines the settings for the JSON serialization of events.</param>
        // ReSharper disable once UnusedMember.Global
        public EventStoreAggregateEventDispatcher(
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
                IDictionary<string, JToken> metadata = JObject.Parse(Encoding.UTF8.GetString(rawMessage.Event.Metadata.Span));

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
                var jsonString = Encoding.UTF8.GetString(rawMessage.Event.Data.Span);
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
}
