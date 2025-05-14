package utils

import "sync"

// TypedSyncMap es un wrapper genérico sobre sync.Map para tipado fuerte y autocompletado
type TypedSyncMap[K comparable, V any] struct {
	m sync.Map
}

func NewTypedSyncMap[K comparable, V any]() *TypedSyncMap[K, V] {
	return &TypedSyncMap[K, V]{m: sync.Map{}}
}

func (t *TypedSyncMap[K, V]) Store(key K, value V) {
	t.m.Store(key, value)
}

func (t *TypedSyncMap[K, V]) Load(key K) (V, bool) {
	val, ok := t.m.Load(key)
	if !ok {
		var zero V
		return zero, false
	}
	v, ok := val.(V)
	return v, ok
}

func (t *TypedSyncMap[K, V]) Delete(key K) {
	t.m.Delete(key)
}

func (t *TypedSyncMap[K, V]) LoadOrStore(key K, value V) (actual V, loaded bool) {
	actualVal, loaded := t.m.LoadOrStore(key, value)
	actual, _ = actualVal.(V)
	return actual, loaded
}

func (t *TypedSyncMap[K, V]) Range(f func(key K, value V) bool) {
	t.m.Range(func(k, v any) bool {
		key, ok1 := k.(K)
		val, ok2 := v.(V)
		if !ok1 || !ok2 {
			return true // salta claves/valores de tipo incorrecto
		}
		return f(key, val)
	})
}
