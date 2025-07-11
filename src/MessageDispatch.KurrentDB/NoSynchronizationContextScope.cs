﻿// Copyright (c) Pharmaxo. All rights reserved.

using System;
using System.Threading;

/* This file is taken from Event Store codebase
   https://github.com/EventStore/samples/blob/main/CQRS_Flow/.NET/Core/Core/Threading/NoSynchronizationContextScope.cs
   As such we should not add a Pharmaxo Scientific copyright file header */

// ReSharper disable InconsistentNaming
#pragma warning disable CS8632, SA1600, SX1309

namespace PharmaxoScientific.MessageDispatch.KurrentDB;

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
        private readonly SynchronizationContext? _synchronizationContext;

        public Disposable(SynchronizationContext? synchronizationContext) => _synchronizationContext = synchronizationContext;

        public void Dispose() =>
            SynchronizationContext.SetSynchronizationContext(_synchronizationContext);
    }
}
#pragma warning restore CS8632, SA1600, SX1309
