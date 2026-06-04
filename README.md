# stream-dup

Provides the ability to duplicate a single `Stream` into an arbitrary number of `Stream`s that yield the same sequence of items as the original stream. Defaults to a `Vec` as its backing store, but you can implement alternate `BackingStore`s.

Example usage:
```
let vec: Vec<i32> = vec![1, 5, 2, 4, 3];
let stream = stream_from_vec(&vec);
let stream_dup = stream.dup();  // Or `StreamDup::new(stream)`
for i in 0..10 {
    let c = &stream_dup.stream().collect::<Vec<i32>>().await
    assert_eq!(&vec, c);
}
```
