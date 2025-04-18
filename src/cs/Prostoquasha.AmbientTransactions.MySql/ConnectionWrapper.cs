using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace Prostoquasha.AmbientTransactions.MySql;

public sealed class ConnectionWrapper(DbConnection connection, bool leaveOpen) : IDisposable, IAsyncDisposable
{
    private readonly bool _leaveOpen = leaveOpen;

    public DbConnection Connection { get; } = connection;

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
