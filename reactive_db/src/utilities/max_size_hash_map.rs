
use std::collections::VecDeque;
use std::collections::HashMap;

pub struct MaxSizeHashMap<K, V> {
    map: HashMap<K, (V, usize)>,
    inserts: VecDeque<K>,
    size: usize
}

#[allow(dead_code)]
impl<K: Eq + std::hash::Hash + Clone, V> MaxSizeHashMap<K, V> {
    pub fn new(max_size: usize) -> MaxSizeHashMap<K, V>{
        MaxSizeHashMap {
            map: HashMap::new(),
            inserts: VecDeque::new(),
            size: max_size
        }
    }

    pub fn get(&mut self, key: &K) -> Option<&V>{
        match self.map.get(key) {
            Some((value, _)) => Some(value),
            None => None
        }
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V>{
        match self.map.get_mut(key) {
            Some((value, _)) => Some(value),
            None => None
        }
    }

    pub fn insert(&mut self, key: K, value: V) {
        let index = self.inserts.len();
        if index < self.size {
            self.inserts.push_back(key.clone());
            self.map.insert(key, (value, index));
        }
        else {
            let removed_key = self.inserts.pop_front().unwrap(); 
            self.map.remove(&removed_key);
            self.insert(key, value);
        }
    }

    pub fn remove(&mut self, key: K) -> Option<V> {
        match self.map.remove(&key) {
            Some((value, index)) => {
                self.inserts.remove(index);
                Some(value)
            },
            None => None
        }
    }
}