# stream-dup

Rust library that provides the ability to duplicate a single `Stream` into an arbitrary number of `Stream`s that yield the same sequence of items as the original stream.

Example usage:
```
let vec: Vec<i32> = vec![1, 5, 2, 4, 3];
let stream = stream_from_vec(&vec);
let stream_dup = stream.dup();  // Alternatively, `StreamDup::new(stream)`
for i in 0..10 {
    assert_eq!(&vec, &stream_dup.stream().collect::<Vec<i32>>().await);
}
```
