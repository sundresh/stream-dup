use std::marker::PhantomData;

#[allow(unused_imports)]
use futures_core::Stream;
use futures_util::FutureExt;
#[allow(unused_imports)]
use crate::stream_dup::StreamDup;

////////////////////////////////////////////////////////////////////////////////////////////////////
/// Backing store for a [`StreamDup`]: an append-only indexed sequence of items.
///
/// In the [`Vec`] implementation of [`BackingStore`], all items passed to [`BackingStore::append`]
/// are returned as-is by [`BackingStore::get`]. Other implementations of [`BackingStore`] may
/// choose to coalesce or split items. For example, a file-based backing store may be designed to
/// return as many bytes that remain in a file, up to some maximum block size.
///
/// `Error` is an optional type parameter rather than an associated type, since default associated
/// types are currently unstable in Rust.
///
/// Example of an infallible [`BackingStore`]:
/// ```
/// # use stream_dup::BackingStore;
/// struct InfallibleBackingStore<T> {
///     contents: Vec<T>
/// }
///
/// impl<T> Default for InfallibleBackingStore<T> {
///     fn default() -> Self {
///         Self { contents: Default::default() }
///     }
/// }
///
/// impl<Item: Clone> BackingStore for InfallibleBackingStore<Item> {
///     type Index = usize;
///     type Item = Item;
///
///     fn get(&self, index: Self::Index) -> impl Future<Output = Option<(Self::Item, Self::Index)>> {
///         std::future::ready(self.contents.as_slice().get(index)
///             .map(|item| (item.clone(), index + 1)))
///     }
///
///     fn append(&mut self, item: Item) -> impl Future<Output = ()> {
///         self.contents.push(item);
///         std::future::ready(())
///     }
/// }
/// ```
///
/// Example of a fallible [`BackingStore`]:
/// ```
/// # use stream_dup::BackingStore;
/// #[derive(Clone)]
/// struct MyError { }
///
/// struct FallibleBackingStore<T> {
///     contents: Vec<T>
/// }
///
/// impl<T> Default for FallibleBackingStore<T> {
///     fn default() -> Self {
///         Self { contents: Default::default() }
///     }
/// }
///
/// impl<Item: Clone> BackingStore<MyError> for FallibleBackingStore<Item> {
///     type Index = usize;
///     type Item = Item;
///
///     fn get(&self, index: Self::Index) -> impl Future<Output = Option<(Self::Item, Self::Index)>> {
///         std::future::ready(self.contents.as_slice().get(index)
///             .map(|item| (item.clone(), index + 1)))
///     }
///
///     fn try_append(&mut self, item: Item) -> impl Future<Output = Result<(), MyError>> {
///         self.contents.push(item);
///         std::future::ready(Ok(()))  // Could also be `Err(MyError { })`
///     }
/// }
/// ```
pub trait BackingStore<Error: Clone = ()>: Default {
    type Index: Clone + Default;
    type Item: Clone;

    /// Gets the item at `index` and the index of the next item, or returns [`None`] if there
    /// currently is no item at the specified index.
    fn get(&self, index: Self::Index) -> impl Future<Output = Option<(Self::Item, Self::Index)>>;

    /// Appends `item` to the backing store, so it can later be returned by [`get`](Self::get).
    /// If you may need to return an `Error`, implement [`try_append`](Self::try_append) instead.
    fn append(&mut self, item: Self::Item) -> impl Future<Output = ()> {
        self.try_append(item).map(|_| ())
    }

    /// Appends `item` to the backing store, so it can later be returned by [`get`](Self::get).
    /// Returns an error if appending fails. If you do not need to return an `Error`, implement
    /// [`append`](Self::append) instead.
    fn try_append(&mut self, item: Self::Item) -> impl Future<Output = Result<(), Error>> {
        self.append(item).map(|_| Ok(()))
    }
}

impl<Item: Clone> BackingStore for Vec<Item> {
    type Index = usize;
    type Item = Item;

    fn get(&self, index: Self::Index) -> impl Future<Output = Option<(Self::Item, Self::Index)>> {
        std::future::ready(self.as_slice().get(index).map(|item| (item.clone(), index + 1)))
    }

    fn append(&mut self, item: Item) -> impl Future<Output = ()> {
        Self::push(self, item);
        std::future::ready(())
    }
}
