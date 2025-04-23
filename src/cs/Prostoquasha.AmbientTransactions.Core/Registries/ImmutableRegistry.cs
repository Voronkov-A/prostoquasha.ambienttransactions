using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Prostoquasha.AmbientTransactions.Core.Registries;

internal sealed class ImmutableRegistry<T> : IRegistry<T>
{
    private readonly Dictionary<string, T> _items;

    public ImmutableRegistry(IEnumerable<string> keys, Func<T> itemFactory)
    {
        _items = keys.ToDictionary(x => x, _ => itemFactory());
    }

    public T GetOrAdd(string key)
    {
        return _items.TryGetValue(key, out var item)
            ? item
            : throw new InvalidOperationException($"Item with key '{key}' has not been registered.");
    }

    public IEnumerator<T> GetEnumerator()
    {
        return _items.Values.GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }
}
