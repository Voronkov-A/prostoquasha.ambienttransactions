using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Prostoquasha.AmbientTransactions.Core;

internal sealed class SharedTransactionState<TConnection>(DbTransaction? dbTransaction, TConnection dbConnection)
    where TConnection : DbConnection
{
    private bool _rolledBack;
    private int _uncommittedCounter;
    private int _referenceCounter;

    public DbTransaction? DbTransaction { get; } = dbTransaction;

    public TConnection DbConnection { get; } = dbConnection;

    public async Task CommitAsync(CancellationToken cancellationToken)
    {
        if (_rolledBack)
        {
            throw new InvalidOperationException("Transaction is rolled back.");
        }

        if (_uncommittedCounter <= 1 && DbTransaction != null)
        {
            await DbTransaction.CommitAsync(cancellationToken);
        }

        --_uncommittedCounter;
    }

    public void Rollback()
    {
        _rolledBack = true;
    }

    public void Acquire()
    {
        ++_uncommittedCounter;
        ++_referenceCounter;
    }

    public async ValueTask ReleaseAsync()
    {
        if (--_referenceCounter <= 0)
        {
            if (DbTransaction != null)
            {
                await DbTransaction.DisposeAsync();
            }

            await DbConnection.DisposeAsync();
        }
    }

    public void Release()
    {
        if (--_referenceCounter <= 0)
        {
            DbTransaction?.Dispose();
            DbConnection.Dispose();
        }
    }
}
