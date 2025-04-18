using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Prostoquasha.AmbientTransactions.MySql;

// TODO: do not share ownership. make outer transaction an unique owner.
internal sealed class SharedTransactionState(DbTransaction? dbTransaction, DbConnection dbConnection)
{
    private bool _rolledBack;
    private int _uncommittedCounter;
    private int _referenceCounter;

    public DbTransaction? DbTransaction { get; } = dbTransaction;

    public DbConnection DbConnection { get; } = dbConnection;

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
