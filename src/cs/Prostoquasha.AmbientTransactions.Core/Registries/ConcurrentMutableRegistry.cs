using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Prostoquasha.AmbientTransactions.Core.Registries;

internal sealed class ConcurrentMutableRegistry<T> : IRegistry<T>
{
    private readonly ConcurrentDictionary<string, T> _items;
    private readonly Func<T> _itemFactory;

    public ConcurrentMutableRegistry(Func<T> itemFactory)
    {
        _items = new ConcurrentDictionary<string, T>();
        _itemFactory = itemFactory;
    }

    public T GetOrAdd(string key)
    {
        return _items.GetOrAdd(key, _ => _itemFactory());
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
