using System;
using System.Threading;
using System.Threading.Tasks;

namespace Prostoquasha.AmbientTransactions.Core;

public interface ITransaction : IDisposable, IAsyncDisposable
{
    Task CommitAsync(CancellationToken cancellationToken);
}
