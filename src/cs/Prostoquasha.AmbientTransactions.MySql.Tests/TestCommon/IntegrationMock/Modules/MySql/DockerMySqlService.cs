using IntegrationMocks.Modules.Sql;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Prostoquasha.AmbientTransactions.MySql.Tests.TestCommon.IntegrationMock.Modules.MySql;

public sealed class DockerMySqlService : IInfrastructureService<SqlServiceContract>
{
    private readonly IPort _port;
    private readonly PostgreSqlContainer _container;

    public DockerMySqlService()
        : this(new RandomNameGenerator(nameof(DockerPostgresService)), PortManager.Default)
    {
    }

    public DockerMySqlService(INameGenerator nameGenerator, IPortManager portManager)
        : this(nameGenerator, portManager, PortRange.Default, new DockerPostgresServiceOptions(), false)
    {
    }

    public DockerMySqlService(INameGenerator nameGenerator, IPortManager portManager, bool attachOutput)
        : this(nameGenerator, portManager, PortRange.Default, new DockerPostgresServiceOptions(), attachOutput)
    {
    }

    public DockerMySqlService(
        INameGenerator nameGenerator,
        IPortManager portManager,
        Range<int> portRange,
        DockerPostgresServiceOptions options,
        bool attachOutput)
    {
        _port = portManager.TakePort(portRange);
        _container = new PostgreSqlBuilder()
            .WithImage(options.Image)
            .WithName(nameGenerator.GenerateName())
            .WithPortBinding(_port.Number, PostgreSqlBuilder.PostgreSqlPort)
            .WithEnvironment("POSTGRES_USER", options.Username)
            .WithEnvironment("POSTGRES_PASSWORD", options.Password)
            .WithAutoRemove(true)
            .WithOutput<PostgreSqlBuilder, PostgreSqlContainer>(attachOutput)
            .Build();

        Contract = new SqlServiceContract(
            username: options.Username,
            password: options.Password,
            host: "localhost",
            port: _port.Number);
    }

    public SqlServiceContract Contract { get; }

    public async ValueTask DisposeAsync()
    {
        if (_container != null)
        {
            await _container.DisposeAsync();
        }

        _port?.Dispose();
        GC.SuppressFinalize(this);
    }

    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        await _container.StartAsync(cancellationToken);
    }

    public void Dispose()
    {
        using (NullSynchronizationContext.Enter())
        {
            DisposeAsync().AsTask().GetAwaiter().GetResult();
        }

        GC.SuppressFinalize(this);
    }

    ~DockerMySqlService()
    {
        Dispose();
    }
}
