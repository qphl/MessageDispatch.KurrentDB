using System;
using System.Text;
using CR.MessageDispatch.Core;
using EventStore.ClientAPI;

namespace CR.MessageDispatch.Dispatchers.EventStore
{
    public class ForgetfulEventStoreDispatcher : IDispatcher<ResolvedEvent>
    {
        private readonly IEventStoreConnection _connection;
        private readonly string _replacementStreamPrefix;
        private readonly IDispatcher<ResolvedEvent> _innerDispatcher;
        
        public ForgetfulEventStoreDispatcher(IEventStoreConnection connection, string replacementStreamPrefix, IDispatcher<ResolvedEvent> innerDispatcher)
        {
            _connection = connection ?? throw new ArgumentException(nameof(connection));
            _replacementStreamPrefix =
                replacementStreamPrefix ?? throw new ArgumentException(nameof(replacementStreamPrefix));
        }
        
        public void Dispatch(ResolvedEvent message)
        {
            // Normal successful read
            if (message.Event != null)
            {
                // A replacement event, which we will presumably have seen before, so skip it
                if (message.Event.EventStreamId.StartsWith(_replacementStreamPrefix))
                    return;
                
                // Otherwise a normal successful read, just pass it on
                _innerDispatcher.Dispatch(message);
                return;
            }
            
            // Otherwise this is a link event which failed to resolve
            // Try to find out where it pointed, skip the event if parsing it fails
            if(!TryDeconstructLink(message.Link, out var deconstructedLink)) return;
            
            // Skip scavenged events from system streams
            if (deconstructedLink.Stream.StartsWith("$")) return;
                
            // Go look for an anonymous steam
            var readResult = _connection.ReadEventAsync(_replacementStreamPrefix + deconstructedLink.Stream,
                deconstructedLink.EventNumber, true).Result;
                
            // We found a replacement anonymised event
            if (readResult.Event.HasValue)
                _innerDispatcher.Dispatch(readResult.Event.Value);
        }

        // Link event bodies are of the form 123@streamname, this function parses these events
        private static bool TryDeconstructLink(RecordedEvent link, out DeconstructedLink contents)
        {
            contents = null;

            if (link == null) return false;
            
            var parts = Encoding.UTF8.GetString(link.Data).Split('@');
            
            if (parts.Length != 2) return false;
            if (!int.TryParse(parts[0], out var eventNumber)) return false;
            if (string.IsNullOrWhiteSpace(parts[1])) return false;
            
            contents = new DeconstructedLink(eventNumber, parts[1]);
            return true;
        }

        private class DeconstructedLink
        {
            public readonly int EventNumber;
            public readonly string Stream;

            public DeconstructedLink(int eventNumber, string stream)
            {
                EventNumber = eventNumber;
                Stream = stream;
            }
        }
    }
}