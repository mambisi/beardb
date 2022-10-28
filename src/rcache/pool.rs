use parking_lot::Mutex;
use std::mem::{ManuallyDrop, forget};
use std::ops::{Deref, DerefMut};

pub type Stack<T> = Vec<T>;

pub struct Pool<T> {
    objects: Mutex<Stack<T>>,
}

impl<T> Pool<T> {
    #[inline]
    pub fn new<F>(cap: usize, init: F) -> Pool<T>
        where
            F: Fn() -> T,
    {
        let mut objects = Stack::new();

        for _ in 0..cap {
            objects.push(init());
        }

        Pool {
            objects: Mutex::new(objects),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.objects.lock().len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.objects.lock().is_empty()
    }

    #[inline]
    pub fn try_pull(&self) -> Option<Reusable<T>> {
        self.objects
            .lock()
            .pop()
            .map(|data| Reusable::new(self, data))
    }

    #[inline]
    pub fn pull<F: Fn() -> T>(&self, fallback: F) -> Reusable<T> {
        self.try_pull()
            .unwrap_or_else(|| Reusable::new(self, fallback()))
    }

    #[inline]
    pub fn attach(&self, t: T) {
        self.objects.lock().push(t)
    }
}

pub struct Reusable<'a, T> {
    pool: &'a Pool<T>,
    data: ManuallyDrop<T>,
}

impl<'a, T> Reusable<'a, T> {
    #[inline]
    pub fn new(pool: &'a Pool<T>, t: T) -> Self {
        Self {
            pool,
            data: ManuallyDrop::new(t),
        }
    }

    #[inline]
    pub fn detach(mut self) -> (&'a Pool<T>, T) {
        let ret = unsafe { (self.pool, self.take()) };
        forget(self);
        ret
    }

    unsafe fn take(&mut self) -> T {
        ManuallyDrop::take(&mut self.data)
    }
}

impl<'a, T> Deref for Reusable<'a, T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<'a, T> DerefMut for Reusable<'a, T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

impl<'a, T> Drop for Reusable<'a, T> {
    #[inline]
    fn drop(&mut self) {
        unsafe { self.pool.attach(self.take()) }
    }
}

#[cfg(test)]
mod tests {
    use std::mem::drop;
    use crate::rcache::pool::{Pool, Reusable};

    #[test]
    fn detach() {
        let pool = Pool::new(1, || Vec::new());
        let (pool, mut object) = pool.try_pull().unwrap().detach();
        object.push(1);
        Reusable::new(&pool, object);
        assert_eq!(pool.try_pull().unwrap()[0], 1);
    }

    #[test]
    fn detach_then_attach() {
        let pool = Pool::new(1, || Vec::new());
        let (pool, mut object) = pool.try_pull().unwrap().detach();
        object.push(1);
        pool.attach(object);
        assert_eq!(pool.try_pull().unwrap()[0], 1);
    }

    #[test]
    fn pull() {
        let pool = Pool::<Vec<u8>>::new(1, || Vec::new());

        let object1 = pool.try_pull();
        let object2 = pool.try_pull();
        let object3 = pool.pull(|| Vec::new());

        assert!(object1.is_some());
        assert!(object2.is_none());
        drop(object1);
        drop(object2);
        drop(object3);
        assert_eq!(pool.len(), 2);
    }

    #[test]
    fn e2e() {
        let pool = Pool::new(10, || Vec::new());
        let mut objects = Vec::new();

        for i in 0..10 {
            let mut object = pool.try_pull().unwrap();
            object.push(i);
            objects.push(object);
        }

        assert!(pool.try_pull().is_none());
        drop(objects);
        assert!(pool.try_pull().is_some());

        for i in 10..0 {
            let mut object = pool.objects.lock().pop().unwrap();
            assert_eq!(object.pop(), Some(i));
        }
    }
}
