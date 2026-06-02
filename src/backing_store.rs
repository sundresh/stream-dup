#[allow(unused_imports)]
use futures_core::Stream;
#[allow(unused_imports)]
use crate::stream_dup::StreamDup;

////////////////////////////////////////////////////////////////////////////////////////////////////
/// Backing store for a [`StreamDup`]: an append-only indexed sequence of items.
///
/// In the [`Vec`] implementation of [`BackingStore`], all items passed to [`BackingStore::append`]
/// are returned as-is by [`BackingStore::get`]. Other implementations of [`BackingStore`] may
/// choose to coalesce or split items. For example, a file-based backing store may be designed to
/// return as many bytes that remain in a file, up to some maximum block size.
pub trait BackingStore: Default {
    type Index: Clone + Default;
    type Item;

    /// Gets the item at `index` and the index of the next item, or returns [`None`] if there
    /// currently is no item at the specified index.
    fn get(&self, index: Self::Index) -> impl Future<Output = Option<(&Self::Item, Self::Index)>>;

    /// Appends `item` to the backing store, so it can later be returned by [`get`](Self::get).
    fn append(&mut self, item: Self::Item) -> impl Future<Output = ()>;
}

impl<Item> BackingStore for Vec<Item> {
    type Index = usize;
    type Item = Item;

    fn get(&self, index: Self::Index) -> impl Future<Output = Option<(&Item, Self::Index)>> {
        std::future::ready(self.as_slice().get(index).map(|item| (item, index + 1)))
    }

    fn append(&mut self, item: Item) -> impl Future<Output = ()> {
        Self::push(self, item);
        std::future::ready(())
    }
}
