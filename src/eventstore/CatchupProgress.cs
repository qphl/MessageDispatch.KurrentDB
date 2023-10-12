// <copyright file="CatchupProgress.cs" company="Corsham Science">
// Copyright (c) Corsham Science. All rights reserved.
// </copyright>

namespace CorshamScience.MessageDispatch.EventStore
{
    /// <summary>
    /// Class to handle calculating catchup progress.
    /// </summary>
    public class CatchupProgress
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="CatchupProgress"/> class.
        /// </summary>
        /// <param name="eventsProcessed">The number of events which have been processed.</param>
        /// <param name="startPosition">The catchup process' starting position in the stream.</param>
        /// <param name="streamName">The name of the stream which is being caught up on.</param>
        /// <param name="totalEvents">The total number of events in the stream which is being caught up on.</param>
        public CatchupProgress(int eventsProcessed, long startPosition, string streamName, long totalEvents)
        {
            EventsProcessed = eventsProcessed;
            StartPosition = startPosition;
            StreamName = streamName;
            TotalEvents = totalEvents;
        }

        /// <summary>
        /// Gets the name of the stream.
        /// </summary>
        public string StreamName { get; }

        /// <summary>
        /// Gets the number of events which have been processed.
        /// </summary>
        public int EventsProcessed { get; }

        /// <summary>
        /// Gets the starting position in the stream.
        /// </summary>
        public long StartPosition { get; }

        /// <summary>
        /// Gets the total number of events in the stream.
        /// </summary>
        public long TotalEvents { get; }

        /// <summary>
        /// Gets the percentage  of events in the stream which have been processed.
        /// </summary>
        public decimal StreamPercentage => EventsProcessed == 0 || TotalEvents == 0
            ? 0.0m
            : ((decimal)StartPosition + EventsProcessed) / TotalEvents * 100;

        /// <summary>
        /// Gets the percentage  of events in the stream which require catching up on, which have been processed.
        /// </summary>
        public decimal CatchupPercentage => EventsProcessed == 0 ? 0.0m : (decimal)EventsProcessed / (TotalEvents - StartPosition) * 100;

        /// <summary>
        /// Generates a string describing the state of the stream catch up progress.
        /// </summary>
        /// <returns>A string describing the state of the stream catch up progress.</returns>
        public override string ToString()
        {
            return
                $"[{StreamName}] Stream Pos: {StreamPercentage:0.#}% ({EventsProcessed + StartPosition}/{TotalEvents}), Caught up: {CatchupPercentage:0.#}% ({EventsProcessed}/{TotalEvents - StartPosition})";
        }
    }
}
