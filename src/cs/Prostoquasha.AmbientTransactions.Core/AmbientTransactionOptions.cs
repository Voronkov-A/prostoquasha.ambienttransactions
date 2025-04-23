using System.Data;

namespace Prostoquasha.AmbientTransactions.Core;

public sealed class AmbientTransactionOptions
{
    public IsolationLevel IsolationLevel { get; init; } = IsolationLevel.ReadCommitted;

    public AmbientTransactionMode Mode { get; init; } = AmbientTransactionMode.Required;

    public IsolationLevel? MaximumIsolationLevel { get; init; }
}
