using AutoFixture;
using Dapper;
using IntegrationMocks.Core;
using IntegrationMocks.Modules.MySql;
using MySql.Data.MySqlClient;
using Prostoquasha.AmbientTransactions.Core.Tests.TestCommon;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Prostoquasha.AmbientTransactions.Core.Tests;

public sealed class MySqlAmbientDbConnectionProviderTests : IAsyncLifetime, IClassFixture<MySqlFixture>
{
    private readonly IInfrastructureService<MySqlServiceContract> _mySql;
    private readonly IAmbientDbConnectionProvider<MySqlConnection> _sut;
    private readonly string _firstConnectionString;
    private readonly string _secondConnectionString;
    private readonly string _firstDatabase;
    private readonly string _secondDatabase;

    public MySqlAmbientDbConnectionProviderTests(MySqlFixture mySqlFixture)
    {
        var fixture = new Fixture();
        _mySql = mySqlFixture.MySql;
        _firstDatabase = fixture.Create<string>();
        _secondDatabase = fixture.Create<string>();
        _firstConnectionString = _mySql.CreateMySqlConnectionString(_firstDatabase);
        _secondConnectionString = _mySql.CreateMySqlConnectionString(_secondDatabase);
        _sut = DbConnectionProvider.CreateNonConcurrent(s => new MySqlConnection(s));
    }

    public async Task InitializeAsync()
    {
        await using var masterConnection = new MySqlConnection(_mySql.CreateMySqlConnectionString());
        await masterConnection.ExecuteAsync(
            $"""
            CREATE DATABASE `{_firstDatabase}`;
            CREATE DATABASE `{_secondDatabase}`;
            """);
        await using var firstConnection = new MySqlConnection(_firstConnectionString);
        await firstConnection.ExecuteAsync(
            """
            CREATE TABLE test (id text);
            """);
        await using var secondConnection = new MySqlConnection(_secondConnectionString);
        await secondConnection.ExecuteAsync(
            """
            CREATE TABLE test (id text);
            """);
    }

    public async Task DisposeAsync()
    {
        await using var masterConnection = new MySqlConnection(_mySql.CreateMySqlConnectionString());
        await masterConnection.ExecuteAsync(
            $"""
            DROP DATABASE `{_firstDatabase}`;
            DROP DATABASE `{_secondDatabase}`;
            """);
    }

    [Fact]
    public async Task CommitAsync_commits_transaction()
    {
        const string id = nameof(CommitAsync_commits_transaction);

        await using (var transaction = await _sut.BeginTransactionAsync(
            _firstConnectionString,
            new AmbientTransactionOptions
            {
                Mode = AmbientTransactionMode.Required,
                IsolationLevel = IsolationLevel.ReadCommitted
            },
            CancellationToken.None))
        {
            await using var connection = _sut.GetConnection(_firstConnectionString);
            await InsertAsync(connection.Connection, id);
            await transaction.CommitAsync(CancellationToken.None);
        }

        await using var assertConnection = new MySqlConnection(_firstConnectionString);
        var item = await SelectAsync(assertConnection, id);
        Assert.Equal(id, item);
    }

    [Fact]
    public async Task Explicit_CommitAsync_is_required()
    {
        const string id = nameof(Explicit_CommitAsync_is_required);

        await using (var transaction = await _sut.BeginTransactionAsync(
            _firstConnectionString,
            new AmbientTransactionOptions
            {
                Mode = AmbientTransactionMode.Required,
                IsolationLevel = IsolationLevel.ReadCommitted
            },
            CancellationToken.None))
        {
            await using var connection = _sut.GetConnection(_firstConnectionString);
            await InsertAsync(connection.Connection, id);
        }

        await using var assertConnection = new MySqlConnection(_firstConnectionString);
        var item = await SelectAsync(assertConnection, id);
        Assert.Null(item);
    }

    [Fact]
    public async Task BeginTransactionAsync_enlists_transaction_when_mode_is_Required()
    {
        const string firstId = nameof(BeginTransactionAsync_enlists_transaction_when_mode_is_Required);
        const string secondId = $"{nameof(BeginTransactionAsync_enlists_transaction_when_mode_is_Required)}_2";
        const string thirdId = $"{nameof(BeginTransactionAsync_enlists_transaction_when_mode_is_Required)}_3";
        string? firstItemFromInnerTransaction;
        string? secondItemFromOuterTransaction;

        await using (var outerTransaction = await _sut.BeginTransactionAsync(
            _firstConnectionString,
            new AmbientTransactionOptions
            {
                Mode = AmbientTransactionMode.Required,
                IsolationLevel = IsolationLevel.ReadCommitted
            },
            CancellationToken.None))
        {
            await using var outerConnection = _sut.GetConnection(_firstConnectionString);
            await InsertAsync(outerConnection.Connection, firstId);

            await using (var innerTransaction = await _sut.BeginTransactionAsync(
                _firstConnectionString,
                new AmbientTransactionOptions
                {
                    Mode = AmbientTransactionMode.Required,
                    IsolationLevel = IsolationLevel.ReadCommitted
                },
                CancellationToken.None))
            {
                await using var innerConnection = _sut.GetConnection(_firstConnectionString);
                firstItemFromInnerTransaction = await SelectAsync(innerConnection.Connection, firstId);
                await InsertAsync(innerConnection.Connection, secondId);
                await innerTransaction.CommitAsync(CancellationToken.None);
            }

            secondItemFromOuterTransaction = await SelectAsync(outerConnection.Connection, secondId);
            await InsertAsync(outerConnection.Connection, thirdId);
            await outerTransaction.CommitAsync(CancellationToken.None);
        }

        await using var assertConnection = new MySqlConnection(_firstConnectionString);
        string? firstItem = await SelectAsync(assertConnection, firstId);
        string? secondItem = await SelectAsync(assertConnection, secondId);
        string? thirdItem = await SelectAsync(assertConnection, thirdId);
        Assert.Equal(firstId, firstItem);
        Assert.Equal(secondId, secondItem);
        Assert.Equal(thirdId, thirdItem);
        Assert.Equal(firstId, firstItemFromInnerTransaction);
        Assert.Equal(secondId, secondItemFromOuterTransaction);
    }

    [Fact]
    public async Task CommitAsync_throws_when_mode_is_Required_and_inner_transaction_is_not_committed()
    {
        const string firstId = nameof(CommitAsync_throws_when_mode_is_Required_and_inner_transaction_is_not_committed);
        const string secondId = $"{nameof(CommitAsync_throws_when_mode_is_Required_and_inner_transaction_is_not_committed)}_2";
        const string thirdId = $"{nameof(CommitAsync_throws_when_mode_is_Required_and_inner_transaction_is_not_committed)}_3";
        string? firstItemFromInnerTransaction;
        string? secondItemFromOuterTransaction;

        await using (var outerTransaction = await _sut.BeginTransactionAsync(
            _firstConnectionString,
            new AmbientTransactionOptions
            {
                Mode = AmbientTransactionMode.Required,
                IsolationLevel = IsolationLevel.ReadCommitted
            },
            CancellationToken.None))
        {
            await using var outerConnection = _sut.GetConnection(_firstConnectionString);
            await InsertAsync(outerConnection.Connection, firstId);

            await using (var innerTransaction = await _sut.BeginTransactionAsync(
                _firstConnectionString,
                new AmbientTransactionOptions
                {
                    Mode = AmbientTransactionMode.Required,
                    IsolationLevel = IsolationLevel.ReadCommitted
                },
                CancellationToken.None))
            {
                await using var innerConnection = _sut.GetConnection(_firstConnectionString);
                firstItemFromInnerTransaction = await SelectAsync(innerConnection.Connection, firstId);
                await InsertAsync(innerConnection.Connection, secondId);
            }

            secondItemFromOuterTransaction = await SelectAsync(outerConnection.Connection, secondId);
            await InsertAsync(outerConnection.Connection, thirdId);
            var act = async () => await outerTransaction.CommitAsync(CancellationToken.None);

            await Assert.ThrowsAsync<InvalidOperationException>(act);
        }

        await using var assertConnection = new MySqlConnection(_firstConnectionString);
        string? firstItem = await SelectAsync(assertConnection, firstId);
        string? secondItem = await SelectAsync(assertConnection, secondId);
        string? thirdItem = await SelectAsync(assertConnection, thirdId);
        Assert.Null(firstItem);
        Assert.Null(secondItem);
        Assert.Null(thirdItem);
        Assert.Equal(firstId, firstItemFromInnerTransaction);
        // due to implementation
        Assert.Equal(secondId, secondItemFromOuterTransaction);
    }

    [Fact]
    public async Task BeginTransactionAsync_does_not_enlist_transaction_when_mode_is_RequiresNew()
    {
        const string firstId = nameof(BeginTransactionAsync_does_not_enlist_transaction_when_mode_is_RequiresNew);
        const string secondId = $"{nameof(BeginTransactionAsync_does_not_enlist_transaction_when_mode_is_RequiresNew)}_2";
        const string thirdId = $"{nameof(BeginTransactionAsync_does_not_enlist_transaction_when_mode_is_RequiresNew)}_3";
        string? firstItemFromInnerTransaction;
        string? secondItemFromOuterTransaction;

        await using (var outerTransaction = await _sut.BeginTransactionAsync(
            _firstConnectionString,
            new AmbientTransactionOptions
            {
                Mode = AmbientTransactionMode.Required,
                IsolationLevel = IsolationLevel.ReadCommitted
            },
            CancellationToken.None))
        {
            await using var outerConnection = _sut.GetConnection(_firstConnectionString);
            await InsertAsync(outerConnection.Connection, firstId);

            await using (var innerTransaction = await _sut.BeginTransactionAsync(
                _firstConnectionString,
                new AmbientTransactionOptions
                {
                    Mode = AmbientTransactionMode.RequiresNew,
                    IsolationLevel = IsolationLevel.ReadCommitted
                },
                CancellationToken.None))
            {
                await using var innerConnection = _sut.GetConnection(_firstConnectionString);
                firstItemFromInnerTransaction = await SelectAsync(innerConnection.Connection, firstId);
                await InsertAsync(innerConnection.Connection, secondId);
                await innerTransaction.CommitAsync(CancellationToken.None);
            }

            secondItemFromOuterTransaction = await SelectAsync(outerConnection.Connection, secondId);
            await InsertAsync(outerConnection.Connection, thirdId);
            await outerTransaction.CommitAsync(CancellationToken.None);
        }

        await using var assertConnection = new MySqlConnection(_firstConnectionString);
        string? firstItem = await SelectAsync(assertConnection, firstId);
        string? secondItem = await SelectAsync(assertConnection, secondId);
        string? thirdItem = await SelectAsync(assertConnection, thirdId);
        Assert.Equal(firstId, firstItem);
        Assert.Equal(secondId, secondItem);
        Assert.Equal(thirdId, thirdItem);
        Assert.Null(firstItemFromInnerTransaction);
        // ReadCommitted => can see
        Assert.Equal(secondId, secondItemFromOuterTransaction);
    }

    [Fact]
    public async Task CommitAsync_does_not_throw_when_mode_is_RequiresNew_and_inner_transaction_is_not_committed()
    {
        const string firstId = nameof(CommitAsync_does_not_throw_when_mode_is_RequiresNew_and_inner_transaction_is_not_committed);
        const string secondId = $"{nameof(CommitAsync_does_not_throw_when_mode_is_RequiresNew_and_inner_transaction_is_not_committed)}_2";
        const string thirdId = $"{nameof(CommitAsync_does_not_throw_when_mode_is_RequiresNew_and_inner_transaction_is_not_committed)}_3";
        string? firstItemFromInnerTransaction;
        string? secondItemFromOuterTransaction;

        await using (var outerTransaction = await _sut.BeginTransactionAsync(
            _firstConnectionString,
            new AmbientTransactionOptions
            {
                Mode = AmbientTransactionMode.Required,
                IsolationLevel = IsolationLevel.ReadCommitted
            },
            CancellationToken.None))
        {
            await using var outerConnection = _sut.GetConnection(_firstConnectionString);
            await InsertAsync(outerConnection.Connection, firstId);

            await using (var innerTransaction = await _sut.BeginTransactionAsync(
                _firstConnectionString,
                new AmbientTransactionOptions
                {
                    Mode = AmbientTransactionMode.RequiresNew,
                    IsolationLevel = IsolationLevel.ReadCommitted
                },
                CancellationToken.None))
            {
                await using var innerConnection = _sut.GetConnection(_firstConnectionString);
                firstItemFromInnerTransaction = await SelectAsync(innerConnection.Connection, firstId);
                await InsertAsync(innerConnection.Connection, secondId);
            }

            secondItemFromOuterTransaction = await SelectAsync(outerConnection.Connection, secondId);
            await InsertAsync(outerConnection.Connection, thirdId);
            await outerTransaction.CommitAsync(CancellationToken.None);
        }

        await using var assertConnection = new MySqlConnection(_firstConnectionString);
        string? firstItem = await SelectAsync(assertConnection, firstId);
        string? secondItem = await SelectAsync(assertConnection, secondId);
        string? thirdItem = await SelectAsync(assertConnection, thirdId);
        Assert.Equal(firstId, firstItem);
        Assert.Null(secondItem);
        Assert.Equal(thirdId, thirdItem);
        Assert.Null(firstItemFromInnerTransaction);
        Assert.Null(secondItemFromOuterTransaction);
    }

    [Fact]
    public async Task BeginTransactionAsync_does_not_enlist_transaction_when_mode_is_Suppress()
    {
        const string firstId = nameof(BeginTransactionAsync_does_not_enlist_transaction_when_mode_is_Suppress);
        const string secondId = $"{nameof(BeginTransactionAsync_does_not_enlist_transaction_when_mode_is_Suppress)}_2";
        const string thirdId = $"{nameof(BeginTransactionAsync_does_not_enlist_transaction_when_mode_is_Suppress)}_3";
        string? firstItemFromInnerTransaction;
        string? secondItemFromOuterTransaction;

        await using (var outerTransaction = await _sut.BeginTransactionAsync(
            _firstConnectionString,
            new AmbientTransactionOptions
            {
                Mode = AmbientTransactionMode.Required,
                IsolationLevel = IsolationLevel.ReadCommitted
            },
            CancellationToken.None))
        {
            await using var outerConnection = _sut.GetConnection(_firstConnectionString);
            await InsertAsync(outerConnection.Connection, firstId);

            await using (var innerTransaction = await _sut.BeginTransactionAsync(
                _firstConnectionString,
                new AmbientTransactionOptions
                {
                    Mode = AmbientTransactionMode.Suppress,
                    IsolationLevel = IsolationLevel.ReadCommitted
                },
                CancellationToken.None))
            {
                await using var innerConnection = _sut.GetConnection(_firstConnectionString);
                firstItemFromInnerTransaction = await SelectAsync(innerConnection.Connection, firstId);
                await InsertAsync(innerConnection.Connection, secondId);
                await innerTransaction.CommitAsync(CancellationToken.None);
            }

            secondItemFromOuterTransaction = await SelectAsync(outerConnection.Connection, secondId);
            await InsertAsync(outerConnection.Connection, thirdId);
            await outerTransaction.CommitAsync(CancellationToken.None);
        }

        await using var assertConnection = new MySqlConnection(_firstConnectionString);
        string? firstItem = await SelectAsync(assertConnection, firstId);
        string? secondItem = await SelectAsync(assertConnection, secondId);
        string? thirdItem = await SelectAsync(assertConnection, thirdId);
        Assert.Equal(firstId, firstItem);
        Assert.Equal(secondId, secondItem);
        Assert.Equal(thirdId, thirdItem);
        Assert.Null(firstItemFromInnerTransaction);
        // ReadCommitted => can see
        Assert.Equal(secondId, secondItemFromOuterTransaction);
    }

    [Fact]
    public async Task CommitAsync_does_not_throw_when_mode_is_Suppress_and_inner_transaction_is_not_committed()
    {
        const string firstId = nameof(CommitAsync_does_not_throw_when_mode_is_Suppress_and_inner_transaction_is_not_committed);
        const string secondId = $"{nameof(CommitAsync_does_not_throw_when_mode_is_Suppress_and_inner_transaction_is_not_committed)}_2";
        const string thirdId = $"{nameof(CommitAsync_does_not_throw_when_mode_is_Suppress_and_inner_transaction_is_not_committed)}_3";
        string? firstItemFromInnerTransaction;
        string? secondItemFromOuterTransaction;

        await using (var outerTransaction = await _sut.BeginTransactionAsync(
            _firstConnectionString,
            new AmbientTransactionOptions
            {
                Mode = AmbientTransactionMode.Required,
                IsolationLevel = IsolationLevel.ReadCommitted
            },
            CancellationToken.None))
        {
            await using var outerConnection = _sut.GetConnection(_firstConnectionString);
            await InsertAsync(outerConnection.Connection, firstId);

            await using (var innerTransaction = await _sut.BeginTransactionAsync(
                _firstConnectionString,
                new AmbientTransactionOptions
                {
                    Mode = AmbientTransactionMode.Suppress,
                    IsolationLevel = IsolationLevel.ReadCommitted
                },
                CancellationToken.None))
            {
                await using var innerConnection = _sut.GetConnection(_firstConnectionString);
                firstItemFromInnerTransaction = await SelectAsync(innerConnection.Connection, firstId);
                await InsertAsync(innerConnection.Connection, secondId);
            }

            secondItemFromOuterTransaction = await SelectAsync(outerConnection.Connection, secondId);
            await InsertAsync(outerConnection.Connection, thirdId);
            await outerTransaction.CommitAsync(CancellationToken.None);
        }

        await using var assertConnection = new MySqlConnection(_firstConnectionString);
        string? firstItem = await SelectAsync(assertConnection, firstId);
        string? secondItem = await SelectAsync(assertConnection, secondId);
        string? thirdItem = await SelectAsync(assertConnection, thirdId);
        Assert.Equal(firstId, firstItem);
        Assert.Equal(secondId, secondItem);
        Assert.Equal(thirdId, thirdItem);
        Assert.Null(firstItemFromInnerTransaction);
        Assert.Equal(secondItem, secondItemFromOuterTransaction);
    }

    [Fact]
    public async Task BeginTransactionAsync_handles_parallel_transactions()
    {
        const string idPrefix = nameof(BeginTransactionAsync_handles_parallel_transactions);
        const int workerCount = 10;
        using var barrier = new Barrier(workerCount);
        using var cancellation = new CancellationTokenSource(TimeSpan.FromMinutes(1));
        var workers = Enumerable.Range(0, workerCount).Select(x => Task.Run(() => DoWorkAsync(x, cancellation.Token)));
        string[] results = await Task.WhenAll(workers);
        await using var assertConnection = new MySqlConnection(_firstConnectionString);
        var committedItems = await SelectAllByPrefixAsync(assertConnection, idPrefix);

        for (int i = 0; i < results.Length; ++i)
        {
            Assert.Equal($"{idPrefix}_{i}", results[i]);
        }
        Assert.Equal(results.Order(), committedItems.Order());

        async Task<string> DoWorkAsync(int workerIndex, CancellationToken cancellationToken)
        {
            await using var transaction = await _sut.BeginTransactionAsync(
                _firstConnectionString,
                new AmbientTransactionOptions
                {
                    Mode = AmbientTransactionMode.Required,
                    IsolationLevel = IsolationLevel.ReadCommitted
                },
                CancellationToken.None);
            await using var connection = _sut.GetConnection(_firstConnectionString);
            await InsertAsync(connection.Connection, $"{idPrefix}_{workerIndex}");
            var existingItems = await SelectAllByPrefixAsync(connection.Connection, idPrefix);
            barrier.SignalAndWait(cancellationToken);
            await transaction.CommitAsync(cancellationToken);
            return existingItems.Single();
        }
    }

    private static async Task InsertAsync(IDbConnection connection, string id)
    {
        await connection.ExecuteAsync($"insert into test values (@Id)", new { Id = id });
    }

    private static async Task<string?> SelectAsync(IDbConnection connection, string id)
    {
        return await connection.QuerySingleOrDefaultAsync<string>(
            "select id from test where id = @Id;",
            new { Id = id });
    }

    private static async Task<IEnumerable<string>> SelectAllByPrefixAsync(IDbConnection connection, string id)
    {
        return await connection.QueryAsync<string>("select id from test where id like @Id;", new { Id = $"{id}%" });
    }
}
