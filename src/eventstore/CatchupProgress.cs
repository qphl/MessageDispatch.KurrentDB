// Copyright (c) Pharmaxo. All rights reserved.

namespace PharmaxoScientific.MessageDispatch.EventStore;

/// <summary>
/// Class to handle calculating catchup progress.
/// </summary>
public class CatchupProgress
{
    /// <summary>
    /// Initializes a new instance of the <see cref="CatchupProgress"/> class.
    /// </summary>
    /// <param name="lastProcessedEventPosition">The last processed event position.</param>
    /// <param name="streamName">The name of the stream which is being caught up on (or $all if this is subscribed to all).</param>
    /// <param name="endOfStreamPosition">The end of the stream position (stream position for stream subscription of commit position for all subscription).</param>
    /// <param name="startPosition">The starting position (Event number for stream subscription or commit position for all subscription).</param>
    /// <param name="subscribeToAll">Whether the subscriber is subscribed to all.</param>
    public CatchupProgress(
        ulong lastProcessedEventPosition,
        string streamName,
        ulong endOfStreamPosition,
        ulong startPosition,
        bool subscribeToAll)
    {
        IsAllSubscription = subscribeToAll;
        LastProcessedEventPosition = lastProcessedEventPosition;
        StartPosition = startPosition;
        StreamName = streamName;
        EndOfStreamPosition = endOfStreamPosition;
    }

    /// <summary>
    /// Gets a value indicating whether the subscriber is subscribed to all.
    /// </summary>
    public bool IsAllSubscription { get; }

    /// <summary>
    /// Gets the name of the stream ($all if this is an all subscription).
    /// </summary>
    public string StreamName { get; }

    /// <summary>
    /// Gets the starting position (Event number for stream subscription or commit position for all subscription).
    /// </summary>
    public ulong StartPosition { get; }

    /// <summary>
    /// Gets the last processed event (stream position for stream subscription of commit position for all subscription).
    /// </summary>
    public ulong LastProcessedEventPosition { get; }

    /// <summary>
    /// Gets the end of the stream position (stream position for stream subscription of commit position for all subscription).
    /// </summary>
    public ulong EndOfStreamPosition { get; }

    /// <summary>
    /// Gets the percentage of events in the stream which have been processed (either by number of events or position in the transaction log).
    /// </summary>
    public decimal OverallPercentage =>
        LastProcessedEventPosition == 0 || EndOfStreamPosition == 0
            ? 0.0m
            : (decimal)LastProcessedEventPosition / EndOfStreamPosition * 100;

    /// <summary>
    /// Gets the percentage of events in the stream which require catching up on, which have been processed (either by number of events or position in the transaction log).
    /// </summary>
    public decimal CatchupPercentage =>
        LastProcessedEventPosition - StartPosition == 0 || EndOfStreamPosition - StartPosition == 0
            ? 0.0m
            : ((decimal)LastProcessedEventPosition - StartPosition) / (EndOfStreamPosition - StartPosition) * 100;

    /// <summary>
    /// Generates a string describing the state of the stream catch up progress.
    /// </summary>
    /// <returns>A string describing the state of the stream catch up progress.</returns>
    public override string ToString()
    {
        return
            $"[{StreamName}] Overall Pos: {OverallPercentage:0.#}% ({LastProcessedEventPosition}/{EndOfStreamPosition}), Caught up: {CatchupPercentage:0.#}% ({LastProcessedEventPosition - StartPosition}/{EndOfStreamPosition - StartPosition})";
    }
}
