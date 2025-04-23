using System;
using System.Collections;
using System.Collections.Generic;

namespace Prostoquasha.AmbientTransactions.Core.Registries;

internal sealed class MutableRegistry<T> : IRegistry<T>
{
    private readonly Dictionary<string, T> _items;
    private readonly Func<T> _itemFactory;

    public MutableRegistry(Func<T> itemFactory)
    {
        _items = new Dictionary<string, T>();
        _itemFactory = itemFactory;
    }

    public T GetOrAdd(string key)
    {
        if (!_items.TryGetValue(key, out var item))
        {
            item = _itemFactory();
            _items[key] = item;
        }

        return item;
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
