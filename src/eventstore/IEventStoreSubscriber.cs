namespace CorshamScience.MessageDispatch.EventStore
{
    /// <summary>
    /// Declares methods for interacting with an <see cref="EventStoreSubscriber"/>.
    /// </summary>
    public interface IEventStoreSubscriber
    {
        /// <summary>
        /// Gets a new catchup progress object.
        /// </summary>
        CatchupProgress CatchupProgress { get; }

        /// <summary>
        /// Gets a value indicating whether the view model is ready or not.
        /// </summary>
        /// <returns>Returns true if catchup is within threshold.</returns>
        bool IsLive { get; }

        /// <summary>
        /// Start the subscriber.
        /// </summary>
        void Start();

        /// <summary>
        /// Shut down the subscription.
        /// </summary>
        void ShutDown();
    }
}
