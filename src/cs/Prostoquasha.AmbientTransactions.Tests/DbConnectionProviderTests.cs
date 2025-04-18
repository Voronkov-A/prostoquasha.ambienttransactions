using System.Threading.Tasks;
using Xunit;

namespace Prostoquasha.AmbientTransactions.Tests;

internal sealed class DbConnectionProviderTests : IAsyncLifetime
{
    public Task DisposeAsync()
    {
        throw new System.NotImplementedException();
    }

    public Task InitializeAsync()
    {
        throw new System.NotImplementedException();
    }
}
