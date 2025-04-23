using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Prostoquasha.AmbientTransactions.Core;

public interface IAmbientDbConnectionProvider<TConnection> : IDisposable, IAsyncDisposable
    where TConnection : DbConnection
{
    Task<ITransaction> BeginTransactionAsync(
        string connectionString,
        AmbientTransactionOptions options,
        CancellationToken cancellationToken);

    ConnectionWrapper<TConnection> GetConnection(string connectionString);
}
