// Copyright (c) Pharmaxo. All rights reserved.

namespace PharmaxoScientific.MessageDispatch.KurrentDB;

/// <summary>
/// Declares methods for interacting with an <see cref="KurrentDbSubscriber"/>.
/// </summary>
public interface IKurrentDbSubscriber
{
    /// <summary>
    /// Gets a new catchup progress object.
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    CatchupProgress CatchupProgress { get; }

    /// <summary>
    /// Gets a value indicating whether the view model is ready or not.
    /// </summary>
    /// <returns>Returns true if catchup is within threshold.</returns>
    // ReSharper disable once UnusedMemberInSuper.Global
    bool IsLive { get; }

    /// <summary>
    /// Start the subscriber.
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    void Start();

    /// <summary>
    /// Shut down the subscription.
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    void ShutDown();
}
