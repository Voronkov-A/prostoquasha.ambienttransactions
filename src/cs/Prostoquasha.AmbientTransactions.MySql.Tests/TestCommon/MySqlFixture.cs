using IntegrationMocks.Core;
using IntegrationMocks.Modules.MySql;
using System;
using System.Threading.Tasks;
using Xunit;

namespace Prostoquasha.AmbientTransactions.MySql.Tests.TestCommon;

public sealed class MySqlFixture : IAsyncLifetime, IAsyncDisposable
{
    public MySqlFixture()
    {
        MySql = new DockerMySqlService();
    }

    internal IInfrastructureService<MySqlServiceContract> MySql { get; }

    public async Task DisposeAsync()
    {
        await ((IAsyncDisposable)this).DisposeAsync();
    }

    public async Task InitializeAsync()
    {
        await MySql.InitializeAsync();
    }

    async ValueTask IAsyncDisposable.DisposeAsync()
    {
        await MySql.DisposeAsync();
    }
}
