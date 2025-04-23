using Prostoquasha.AmbientTransactions.Core.Registries;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Prostoquasha.AmbientTransactions.Core;

public sealed class AmbientDbConnectionProvider<TConnection> : IAmbientDbConnectionProvider<TConnection>
    where TConnection : DbConnection
{
    private readonly Func<string, TConnection> _connectionFactory;
    private readonly IRegistry<AsyncLocalStack<TransactionSlot>> _transactionStacks;

    private AmbientDbConnectionProvider(
        IRegistry<AsyncLocalStack<TransactionSlot>> transactionStacks,
        Func<string, TConnection> connectionFactory)
    {
        _transactionStacks = transactionStacks;
        _connectionFactory = connectionFactory;
    }

    public void Dispose()
    {
        var transactions = _transactionStacks.SelectMany(x => x).Select(x => x.Transaction).ToList();

        foreach (var transaction in transactions)
        {
            transaction?.Dispose();
        }
    }

    public async ValueTask DisposeAsync()
    {
        var transactions = _transactionStacks.SelectMany(x => x).Select(x => x.Transaction).ToList();

        foreach (var transaction in transactions)
        {
            if (transaction != null)
            {
                await transaction.DisposeAsync();
            }
        }
    }

    // WARNING: this method must not be translated into async state machine, because of AsyncLocal usage.
    public Task<ITransaction> BeginTransactionAsync(
        string connectionString,
        AmbientTransactionOptions options,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(options);

        var slot = CreateSlot(connectionString);
        return SetTransactionAsync(slot, connectionString, options, cancellationToken);
    }

    public ConnectionWrapper<TConnection> GetConnection(string connectionString)
    {
        var stack = _transactionStacks.GetOrAdd(connectionString);

        if (TryPeekTransaction(stack, out var transaction))
        {
            return new ConnectionWrapper<TConnection>(transaction.State.DbConnection, leaveOpen: true);
        }

        return new ConnectionWrapper<TConnection>(_connectionFactory(connectionString), leaveOpen: false);
    }

    internal static AmbientDbConnectionProvider<TConnection> CreateNonConcurrent(
        Func<string, TConnection> connectionFactory)
    {
        var transactionStacks = new MutableRegistry<AsyncLocalStack<TransactionSlot>>(
            () => new AsyncLocalStack<TransactionSlot>());
        return new AmbientDbConnectionProvider<TConnection>(transactionStacks, connectionFactory);
    }

    internal static AmbientDbConnectionProvider<TConnection> CreateConcurrent(
        Func<string, TConnection> connectionFactory)
    {
        var transactionStacks = new ConcurrentMutableRegistry<AsyncLocalStack<TransactionSlot>>(
            () => new AsyncLocalStack<TransactionSlot>());
        return new AmbientDbConnectionProvider<TConnection>(transactionStacks, connectionFactory);
    }

    internal static AmbientDbConnectionProvider<TConnection> CreateImmutable(
        IEnumerable<string> connectionStrings,
        Func<string, TConnection> connectionFactory)
    {
        var transactionStacks = new ImmutableRegistry<AsyncLocalStack<TransactionSlot>>(
            connectionStrings,
            () => new AsyncLocalStack<TransactionSlot>());
        return new AmbientDbConnectionProvider<TConnection>(transactionStacks, connectionFactory);
    }

    private TransactionSlot CreateSlot(string connectionString)
    {
        var stack = _transactionStacks.GetOrAdd(connectionString);
        var slot = new TransactionSlot();
        stack.Push(slot);
        return slot;
    }

    private async Task<ITransaction> SetTransactionAsync(
        TransactionSlot slot,
        string connectionString,
        AmbientTransactionOptions options,
        CancellationToken cancellationToken)
    {
        try
        {
            switch (options.Mode)
            {
                case AmbientTransactionMode.Required:
                    {
                        var stack = _transactionStacks.GetOrAdd(connectionString);

                        if (TryPeekTransaction(stack, out var transaction)
                            && transaction.State.DbTransaction != null)
                        {
                            ValidateIsolationLevel(transaction, options);
                            slot.Transaction = new ManagedTransaction(this, transaction.State, connectionString);
                        }
                        else
                        {
                            slot.Transaction = await CreateTransactionAsync(
                                connectionString,
                                options.IsolationLevel,
                                cancellationToken);
                        }
                        break;
                    }
                case AmbientTransactionMode.RequiresNew:
                    {
                        slot.Transaction = await CreateTransactionAsync(
                            connectionString,
                            options.IsolationLevel,
                            cancellationToken);
                        break;
                    }
                case AmbientTransactionMode.Suppress:
                    {
                        slot.Transaction = await CreateTransactionAsync(
                            connectionString,
                            null,
                            cancellationToken);
                        break;
                    }
                default:
                    {
                        throw new InvalidOperationException(
                            $"{nameof(AmbientTransactionMode)}.{options.Mode} is not supported.");
                    }
            }

            return slot.Transaction;
        }
        catch
        {
            slot.TransactionRemoved = true;
            throw;
        }
    }

    private async Task<ManagedTransaction> CreateTransactionAsync(
        string connectionString,
        IsolationLevel? isolationLevel,
        CancellationToken cancellationToken)
    {
        TConnection? connection = null;
        DbTransaction? transaction = null;

        try
        {
            connection = _connectionFactory(connectionString);
            await connection.OpenAsync(cancellationToken);

            if (isolationLevel != null)
            {
                transaction = await connection.BeginTransactionAsync(isolationLevel.Value, cancellationToken);
            }

            var state = new SharedTransactionState<TConnection>(transaction, connection);
            return new ManagedTransaction(this, state, connectionString);
        }
        catch
        {
            if (transaction != null)
            {
                await transaction.DisposeAsync();
            }

            if (connection != null)
            {
                await connection.DisposeAsync();
            }

            throw;
        }
    }

    private void RemoveTransaction(ManagedTransaction transaction)
    {
        var stack = _transactionStacks.GetOrAdd(transaction.ConnectionString);

        foreach (var stackItem in stack)
        {
            if (stackItem.Transaction == transaction)
            {
                stackItem.TransactionRemoved = true;
                return;
            }
        }

        while (stack.TryPeek(out var stackItem) && stackItem.TransactionRemoved)
        {
            stack.Pop();
        }
    }

    private static void ValidateIsolationLevel(ManagedTransaction transaction, AmbientTransactionOptions options)
    {
        if (transaction.State.DbTransaction == null)
        {
            throw new InvalidOperationException("Current transaction is suppressed.");
        }

        var minimum = options.IsolationLevel;
        var maximum = options.MaximumIsolationLevel ?? options.IsolationLevel;
        var current = transaction.State.DbTransaction.IsolationLevel;

        if (current < minimum || transaction.State.DbTransaction.IsolationLevel > maximum)
        {
            throw new InvalidOperationException(
                $"Current transaction isolation level {current} does not match the expectations [{minimum}; {maximum}].");
        }
    }

    private static bool TryPeekTransaction(
        AsyncLocalStack<TransactionSlot> stack,
        [MaybeNullWhen(false)] out ManagedTransaction result)
    {
        while (stack.TryPeek(out var slot))
        {
            if (!slot.TransactionRemoved && slot.Transaction != null)
            {
                result = slot.Transaction;
                return true;
            }

            stack.Pop();
        }

        result = default;
        return false;
    }

    private sealed class TransactionSlot
    {
        public ManagedTransaction? Transaction { get; set; }

        public bool TransactionRemoved { get; set; }
    }

    private sealed class ManagedTransaction : ITransaction
    {
        private readonly AmbientDbConnectionProvider<TConnection> _provider;
        private bool _disposed;
        private bool _committed;

        public ManagedTransaction(
            AmbientDbConnectionProvider<TConnection> provider,
            SharedTransactionState<TConnection> state,
            string connectionString)
        {
            _provider = provider;
            ConnectionString = connectionString;
            State = state;

            state.Acquire();
        }

        public string ConnectionString { get; }

        public SharedTransactionState<TConnection> State { get; }

        public async Task CommitAsync(CancellationToken cancellationToken)
        {
            if (_committed)
            {
                return;
            }

            await State.CommitAsync(cancellationToken);
            _committed = true;
        }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            if (!_committed)
            {
                State.Rollback();
            }

            _provider.RemoveTransaction(this);
            State.Release();
            _disposed = true;
        }

        public async ValueTask DisposeAsync()
        {
            if (_disposed)
            {
                return;
            }

            if (!_committed)
            {
                State.Rollback();
            }

            _provider.RemoveTransaction(this);
            await State.ReleaseAsync();
            _disposed = true;
        }
    }
}

public static class DbConnectionProvider
{
    public static AmbientDbConnectionProvider<TConnection> CreateNonConcurrent<TConnection>(
        Func<string, TConnection> connectionFactory)
        where TConnection : DbConnection
    {
        return AmbientDbConnectionProvider<TConnection>.CreateNonConcurrent(connectionFactory);
    }

    public static AmbientDbConnectionProvider<TConnection> CreateConcurrent<TConnection>(
        Func<string, TConnection> connectionFactory)
        where TConnection : DbConnection
    {
        return AmbientDbConnectionProvider<TConnection>.CreateConcurrent(connectionFactory);
    }

    public static AmbientDbConnectionProvider<TConnection> CreateImmutable<TConnection>(
        IEnumerable<string> connectionStrings,
        Func<string, TConnection> connectionFactory)
        where TConnection : DbConnection
    {
        return AmbientDbConnectionProvider<TConnection>.CreateImmutable(connectionStrings, connectionFactory);
    }
}
