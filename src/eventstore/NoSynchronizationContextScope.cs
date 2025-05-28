/* This file is taken from Event Store codebase
   https://github.com/EventStore/samples/blob/main/CQRS_Flow/.NET/Core/Core/Threading/NoSynchronizationContextScope.cs
   As such we should not add a Pharmaxo Scientific copyright file header */

// ReSharper disable InconsistentNaming
#pragma warning disable CS8632, SA1600, SX1309

namespace PharmaxoScientific.MessageDispatch.EventStore
{
    using System;
    using System.Threading;

    internal static class NoSynchronizationContextScope
    {
        public static Disposable Enter()
        {
            var context = SynchronizationContext.Current;
            SynchronizationContext.SetSynchronizationContext(null);
            return new Disposable(context);
        }

        public struct Disposable : IDisposable
        {
            private readonly SynchronizationContext? synchronizationContext;

            public Disposable(SynchronizationContext? synchronizationContext)
            {
                this.synchronizationContext = synchronizationContext;
            }

            public void Dispose() =>
                SynchronizationContext.SetSynchronizationContext(synchronizationContext);
        }
    }
}
#pragma warning restore CS8632, SA1600, SX1309
