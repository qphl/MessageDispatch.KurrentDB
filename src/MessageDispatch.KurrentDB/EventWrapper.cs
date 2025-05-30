// Copyright (c) Pharmaxo. All rights reserved.

using Newtonsoft.Json.Linq;

namespace PharmaxoScientific.MessageDispatch.KurrentDB;

/// <summary>
/// Represents a wrapper for an event.
/// </summary>
public class EventWrapper
{
    /// <summary>
    /// Initializes a new instance of the <see cref="EventWrapper"/> class.
    /// </summary>
    /// <param name="event">The event to wrap.</param>
    /// <param name="previouslyProcessed">A value indicating whether the event has been previously processed.</param>
    public EventWrapper(JObject @event, bool previouslyProcessed)
    {
        Event = @event;
        PreviouslyProcessed = previouslyProcessed;
    }

    /// <summary>
    /// Gets a value indicating whether this event has been previously processed before.
    /// </summary>
    public bool PreviouslyProcessed { get; }

    /// <summary>
    /// Gets the event data.
    /// </summary>
    public JObject Event { get; } = null!;

    /// <inheritdoc />
    public override string ToString() => $"Event: {Event}, PreviouslyProcessed: {PreviouslyProcessed}";
}
