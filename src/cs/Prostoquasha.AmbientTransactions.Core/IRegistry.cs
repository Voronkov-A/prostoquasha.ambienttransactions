using System.Collections.Generic;

namespace Prostoquasha.AmbientTransactions.Core;

internal interface IRegistry<T> : IEnumerable<T>
{
    T GetOrAdd(string key);
}
