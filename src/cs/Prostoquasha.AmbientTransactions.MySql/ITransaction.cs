using System;
using System.Threading;
using System.Threading.Tasks;

namespace Prostoquasha.AmbientTransactions.MySql;

public interface ITransaction : IDisposable, IAsyncDisposable
{
    Task CommitAsync(CancellationToken cancellationToken);
}
