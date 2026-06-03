////////////////////////////////////////////////////////////////////////////////////////////////////
//! Provides the ability to duplicate a single [`Stream`] into an arbitrary number of [`Stream`]s
//! that yield the same sequence of items as the original stream. Defaults to a [`Vec`] as its
//! backing store, but you can implement alternate [`BackingStore`]s.
//! 
//! Example usage:
//! ```
//! # use futures_util::StreamExt;
//! # use tokio::sync::mpsc;
//! # use tokio_stream::wrappers::UnboundedReceiverStream;
//! # use crate::stream_dup::{StreamDupExt, stream_from_vec};
//! # #[tokio::main]
//! # async fn main() {
//! let vec: Vec<i32> = vec![1, 5, 2, 4, 3];
//! let stream = stream_from_vec(&vec);
//! let stream_dup = stream.dup();  // Alternatively, `StreamDup::new(stream)`
//! for i in 0..10 {
//!     assert_eq!(&vec, &stream_dup.stream().collect::<Vec<i32>>().await);
//! }
//! # }
//! ```

use std::{pin::Pin, sync::Arc};
use async_stream::stream;
use futures_core::Stream;
use futures_util::StreamExt;
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tokio_stream::wrappers::UnboundedReceiverStream;
use crate::backing_store::BackingStore;

/// Convenience function to create a [`Stream`] from a [`Vec`] of items. If you want to create a
/// [`StreamDup`] from a [`Vec`], use [`StreamDup::from`] instead.
pub fn stream_from_vec<T: Clone>(vec: &Vec<T>) -> impl Stream<Item = T> + use<T> {
    let (sender, receiver) = mpsc::unbounded_channel::<T>();
    for n in vec {
        sender.send(n.clone()).expect("receiver should never have been dropped/closed");
    }
    UnboundedReceiverStream::new(receiver)
}

////////////////////////////////////////////////////////////////////////////////////////////////////
/// Private contents of a [`StreamDup`]
struct StreamDupContents<Item, B: BackingStore<Item = Item>> {
    /// Items that have already been loaded from the input stream.
    loaded_items: B,
    /// Source of additional items to be loaded. If it is [`None`], then
    /// [`loaded_items`](Self::loaded_items) contains all of the items already.
    input_stream: Option<Pin<Box<dyn Stream<Item = Item> + Send>>>,
}

impl<Item: Clone, B: BackingStore<Item = Item>> StreamDupContents<Item, B> {
    /// Gets the item at `index` and the index of the next item, either from the already loaded
    /// items or by waiting on the input stream. Returns `None` if there are no more items.
    async fn get(&mut self, index: B::Index) -> Option<(Item, B::Index)> {
        loop {
            if let Some((item, next_index)) = self.loaded_items.get(index.clone()).await {
                return Some((item, next_index))
            } else if let Some(input_stream) = self.input_stream.as_mut() {
                if let Some(item) = input_stream.next().await {
                    self.loaded_items.append(item).await;
                } else {
                    self.input_stream = None;
                    return None;
                }
            } else {
                return None;
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
/// `StreamDup` provides the ability to duplicate a single [`Stream`] into an arbitrary number of
/// [`Stream`]s that yield the same sequence of items as the original stream.
/// 
/// Each invocation of the [`stream`](Self::stream) method creates a new duplicate stream.
/// 
/// The [`StreamDupExt`] trait extends types that implement [`Stream`] with the
/// [`dup`](StreamDupExt::dup) method, which converts the stream to a `StreamDup`. The call
/// `s.dup()` is shorthand for `StreamDup::new(s)`.
/// 
/// By default, all of the items from the input stream are stored in memory in a [`Vec`]. For very
/// large streams, consider implementing a different [`BackingStore`] that indirectly accesses data
/// that may either be cached in memory or stored on disk.
#[derive(Clone)]
pub struct StreamDup<Item: Clone + Send + 'static, B: BackingStore<Item = Item> = Vec<Item>> {
    contents: Arc<RwLock<StreamDupContents<Item, B>>>,
}

impl<Item: Clone + Send + 'static, B: BackingStore<Item = Item>> StreamDup<Item, B> {
    /// Create a new `StreamDup` from the items yielded by a [`Stream`].
    pub fn new<S: Stream<Item = Item> + Send + 'static>(stream: S) -> Self {
        let stream_dup = Self {
            contents: Arc::new(RwLock::new(StreamDupContents { loaded_items: B::default(), input_stream: Some(Box::pin(stream)) })),
        };
        stream_dup
    }

    /// Gets the item at `index` and the index of the next item, either from the already loaded
    /// items or by waiting on the input stream. Returns `None` if there are no more items.
    async fn get(&self, index: B::Index) -> Option<(Item, B::Index)> {
        self.contents.write().await.get(index).await
    }

    /// Produces a stream that iterates over the contents of the async vector.
    pub fn stream(&self) -> impl Stream<Item = Item> + use<'_, Item, B> {
        stream! {
            let mut index = B::Index::default();
            while let Some((item, next_index)) = self.get(index).await {
                index = next_index;
                yield item;
            }
        }
    }
}

impl<Item: Clone + Send + 'static, B: BackingStore<Item = Item>> From<B> for StreamDup<Item, B> {
    /// Creates a complete [`StreamDup`] from a [`BackingStore`]. The streams returned by
    /// [`stream`](Self::stream) will never block.
    fn from(items: B) -> Self {
        Self {
            contents: Arc::new(RwLock::new(StreamDupContents { loaded_items: items, input_stream: None })),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
/// `StreamDupExt` extends types that implement [`Stream`] with the [`dup`](StreamDupExt::dup)
/// method, which returns a [`StreamDup`] that provides the ability to create multiple streams that
/// yield the same sequence of items as the original stream.
///
/// This uses the default [`Vec`] backing store. To use a different [`BackingStore`], call
/// [`StreamDup::new`] directly.
pub trait StreamDupExt<Item: Clone + Send> {
    /// Converts this [`Stream`] to a [`StreamDup`] struct whose [`stream`](StreamDup::stream())
    /// method can be used to create multiple streams of the same items.
    fn dup(self) -> StreamDup<Item>;
}

impl<Item: Clone + Send, S: Stream<Item = Item> + Send + 'static> StreamDupExt<Item> for S {
    fn dup(self) -> StreamDup<Item> {
        StreamDup::new(self)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests

#[cfg(test)]
mod tests {
    use std::pin::pin;
    use std::time::Duration;
    use super::*;

    #[tokio::test]
    async fn test_stream_dup_from_vec_to_stream() {
        let vec = vec![1, 5, 2, 4, 3];
        let stream_dup = StreamDup::from(vec.clone());
        let collected_vec: Vec<i32> = stream_dup.stream().collect().await;
        assert_eq!(vec, collected_vec);
    }

    #[tokio::test]
    async fn test_stream_dup_write_and_read_concurrently() {
        let vec: Vec<i32> = vec![1, 5, 2, 4, 3];
        let (sender, receiver) = mpsc::unbounded_channel::<i32>();
        let stream_dup = UnboundedReceiverStream::new(receiver).dup();
        let stream = stream_dup.stream();
        let vec_clone = vec.clone();
        tokio::spawn(async move {
            for i in vec_clone {
                sender.send(i).unwrap();
                tokio::time::sleep(Duration::from_millis(10)).await;  // Tests that stream readers will wait for more items
            }
            tokio::time::sleep(Duration::from_millis(1000)).await;  // Tests that stream readers will wait for more items
        });
        let collected_vec: Vec<i32> = stream.collect().await;
        assert_eq!(vec, collected_vec);
    }

    #[tokio::test]
    async fn test_stream_dup_read_and_read_concurrently() {
        let vec = vec![1, 5, 2, 4, 3];
        let stream_dup = Arc::new(StreamDup::from(vec.clone()));
        let mut stream1 = pin!(stream_dup.stream());
        let mut stream2 = pin!(stream_dup.stream());
        stream1.next().await;
        stream2.next().await;
    }
}
