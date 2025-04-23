using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace Prostoquasha.AmbientTransactions.Core;

public sealed class ConnectionWrapper<TConnection>(TConnection connection, bool leaveOpen) :
    IDisposable,
    IAsyncDisposable
    where TConnection : DbConnection
{
    private readonly bool _leaveOpen = leaveOpen;

    public TConnection Connection { get; } = connection;

    public void Dispose()
    {
        if (!_leaveOpen)
        {
            Connection.Dispose();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (!_leaveOpen)
        {
            await Connection.DisposeAsync();
        }
    }
}
