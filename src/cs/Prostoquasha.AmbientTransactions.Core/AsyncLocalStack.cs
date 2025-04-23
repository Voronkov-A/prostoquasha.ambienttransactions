using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading;

namespace Prostoquasha.AmbientTransactions.Core;

internal sealed class AsyncLocalStack<T> : IEnumerable<T>
{
    private readonly AsyncLocal<Item?> _item = new();

    public void Push(T item)
    {
        _item.Value = new Item(item, _item.Value);
    }

    public bool TryPeek([MaybeNullWhen(false)] out T result)
    {
        if (_item.Value == null)
        {
            result = default;
            return false;
        }

        result = _item.Value.Value;
        return true;
    }

    public T Pop()
    {
        var topItem = _item.Value ?? throw new InvalidOperationException("The stack is empty.");
        _item.Value = topItem.Previous;
        return topItem.Value;
    }

    public IEnumerator<T> GetEnumerator()
    {
        return new Enumerator(this);
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    private sealed record Item(T Value, Item? Previous);

    private sealed class Enumerator(AsyncLocalStack<T> stack) : IEnumerator<T>
    {
        private readonly AsyncLocalStack<T> _stack = stack;
        private Item? _previousItem = stack._item.Value;
        private Item? _currentItem;

        public T Current =>
            _currentItem == null
                ? throw new InvalidOperationException("Invoke MoveNext() first.")
                : _currentItem.Value;

        object? IEnumerator.Current => Current;

        public void Dispose()
        {
            // pass
        }

        public bool MoveNext()
        {
            if (_previousItem == null)
            {
                return false;
            }

            _currentItem = _previousItem;
            _previousItem = _currentItem.Previous;
            return true;
        }

        public void Reset()
        {
            _previousItem = _stack._item.Value;
            _currentItem = null;
        }
    }
}
