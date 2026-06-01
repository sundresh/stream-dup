////////////////////////////////////////////////////////////////////////////////////////////////////
//! Provides the ability to duplicate a single [`Stream`] into an arbitrary number of [`Stream`]s
//! that yield the same sequence of items as the original stream.
//! 
//! Example usage:
//! ```
//! # use futures_util::StreamExt;
//! # use tokio::sync::mpsc;
//! # use tokio_stream::wrappers::UnboundedReceiverStream;
//! # use stream_dup::{StreamDupExt, stream_from_vec};
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
struct StreamDupContents<Item> {
    /// Items that have already been loaded from the input stream.
    loaded_items: Vec<Item>,
    /// Source of additional items to be loaded. If it is [`None`], then
    /// [`loaded_items`](Self::loaded_items) contains all of the items already.
    input_stream: Option<Pin<Box<dyn Stream<Item = Item> + Send>>>,
}

impl<Item: Clone> StreamDupContents<Item> {
    /// Are all items loaded or could more items be loaded from the input stream later?
    fn is_complete(&self) -> bool {
        self.input_stream.is_none()
    }

    /// Gets the item at the specified `index` if it has already been loaded. If [`None`] is returned
    /// but [`is_complete`](Self::is_complete) is [`false`], an item may later be available at
    /// `index`: use [`get`](Self::get) to wait until either it is avaiable or we get to the end
    /// of the input stream.
    fn get_loaded(&self, index: usize) -> Option<Item> {
        self.loaded_items.get(index).map(|item| item.clone())
    }

    /// Gets the item at the specified `index`, either from the already loaded items or by waiting
    /// on the input stream. Returns `None` if we get to the end of the input stream before `index`.
    async fn get(&mut self, index: usize) -> Option<Item> {
        while self.loaded_items.len() <= index && let Some(input_stream) = self.input_stream.as_mut() {
            if let Some(item) = input_stream.next().await {
                self.loaded_items.push(item);
            } else {
                self.input_stream = None;
                return None;
            }
        }
        self.get_loaded(index)
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
/// Note that all of the items from the input stream are stored in memory in a [`Vec`]. For very
/// large streams, consider using items that indirectly access data that may either be cached in
/// memory or stored on disk.
#[derive(Clone)]
pub struct StreamDup<Item: Clone + Send + 'static> {
    contents: Arc<RwLock<StreamDupContents<Item>>>,
}

impl<Item: Clone + Send + 'static> StreamDup<Item> {
    /// Create a new `StreamDup` from the items yielded by a [`Stream`].
    pub fn new<S: Stream<Item = Item> + Send + 'static>(stream: S) -> Self {
        let stream_dup = Self {
            contents: Arc::new(RwLock::new(StreamDupContents { loaded_items: Vec::new(), input_stream: Some(Box::pin(stream)) })),
        };
        stream_dup
    }

    /// Gets the item at the specified `index` if it has already been loaded. If `(`[`None`]`, `[`true`]`)` is
    /// returned, an item may later be available at `index`: use [`get`](Self::get) to wait until
    /// either it is avaiable or we get to the end of the input stream.
    async fn get_loaded(&self, index: usize) -> (Option<Item>, bool) {
        let guard = self.contents.read().await;
        (guard.get_loaded(index), !guard.is_complete())
    }

    /// Gets the item at the specified `index`, either from the already loaded data or by waiting on the input stream.
    async fn get(&self, index: usize) -> Option<Item> {
        self.contents.write().await.get(index).await
    }

    /// Produces a stream that iterates over the contents of the async vector.
    pub fn stream(&self) -> impl Stream<Item = Item> + use<'_, Item> {
        stream! {
            let mut index = 0;
            loop {
                let (item_option, more_coming_later) = self.get_loaded(index).await;
                if let Some(item) = item_option {
                    index += 1;
                    yield item;
                } else if more_coming_later && let Some(item) = self.get(index).await {
                    index += 1;
                    yield item;
                } else {
                    break;
                }
            }
        }
    }
}

impl<Item: Clone + Send + 'static> From<Vec<Item>> for StreamDup<Item> {
    /// Creates a complete [`StreamDup`] from a [`Vec`]. The streams returned by
    /// [`stream`](Self::stream) will never block.
    fn from(items: Vec<Item>) -> Self {
        Self {
            contents: Arc::new(RwLock::new(StreamDupContents { loaded_items: items, input_stream: None })),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////
/// `StreamDupExt` extends types that implement [`Stream`] with the [`dup`](StreamDupExt::dup)
/// method, which returns a [`StreamDup`] that provides the ability to create multiple streams that
/// yield the same sequence of items as the original stream.
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
