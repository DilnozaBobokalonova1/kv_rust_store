use std::collections::HashMap;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{self, AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::Mutex;
use lru::LruCache;

const MAX_CACHE_ENTRIES: usize = 1000;

/* Records the position and length of an entry in the Log File. */
struct LogEntry {
    pos: u64,
    len: u64,
}

/**
 * Use of an in-memory hash map (index) to track the position & length of 
 * each entry in the log file, allowing for fast lookups.
 */
struct KeyValueStore {
    //enables quick lookup of values by key without needing to re-read the file
    index: HashMap<String, LogEntry>,
    log: File,
    //necessary for compaction
    log_path: String,
    // Cache with async Mutex
    cache: Mutex<LruCache<String, Option<String>>>
}

impl KeyValueStore {

    /**
     * Initialize a new instance of the KeyValue store by setting up an underlying
     * log file for the KV store and loading the existing entries into an in-mem index.
     * 
     * # Parameters
     * - `file_path`: A string slice (`&str`) that specifies the path to the log file used by the KV store. 
     * 
     * # Returns
     * An `io::Result<Self>`, representing successful initialization of the `KeyValueStore` instance
     * or an error if the initialization process fails.
     * 
     * Note: It makes sense we impl the new function as async since we are essentially
     * assuming that the KV Store might be initialized from within a larger system. 
     * A blocking initialization could lead to performance bottlenecks or decreased 
     * responsiveness, hence we make our KV store initialization non-blocking.
     */
    async fn new(file_path: &str) -> io::Result<Self> {
        let log: File = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(file_path)
            .await?;

        let mut store = KeyValueStore {
            index: HashMap::new(),
            log,
            log_path: file_path.to_string(),
            cache: Mutex::new(LruCache::new(MAX_CACHE_ENTRIES)),
        };
        store.load().await?;
        Ok(store)
    }

    /**
     * Upon initialization, async call to load reads the log file and (re)builds the index,
     * ensuring that the hash indices are up to date with the log content. Reads the entire
     * log file at the start of the KeyValueStore instance and (re)builds the index.
     * It is responsible for parsing the log file line by line to extract key-value pairs,
     * where each entry is expected to be in the format "key=value\n". It updates the in-memory index
     * with the position and length of each entry, allowing for fast lookups without needing to
     * re-read the file.
     * 
     * # Behavior
     * - The function seeks to the beginning of the log file and reads its content into a buffer.
     * - It then iterates over the buffer, identifying each line (entry) by looking for newline
     *   characters and splits each line into key and value based on the '=' character.
     * - For each valid key-value pair found, it records the position and length of the entry in the
     *   log file and updates the index with this information.
     * 
     * # Error Handling
     * - If the log file cannot be read (e.g., due to I/O errors), an `io::Error` is returned.
     * 
     * # Returns
     * - `Ok(())` on successful parsing and indexing of the log file.
     * - `Err(io::Error)` if an error occurs while reading the file or processing its contents.
     * 
     * Note: This function assumes that the log file's format is correct and does not perform
     * extensive validation of each entry. Malformed entries may result in incomplete or incorrect
     * indexing. However, since we do not have guaranteed UTF-8 sequences, it uses the utf8_lossy
     * method for to_string() conversion, enabling the replacement of invalid UTF-8 chars into �. 
     */

    async fn load(&mut self) -> io::Result<()> {
        self.log.seek(io::SeekFrom::Start(0)).await?;
        let mut buffer = Vec::new();
        self.log.read_to_end(&mut buffer).await?;

        let mut pos = 0;
        while pos < buffer.len() {
            let mut end_pos = pos;
            while end_pos < buffer.len() && buffer[end_pos] != b'\n' {
                end_pos += 1
            }

            //get the full line in two parts of key and value (value with newline **delete newline)
            let parts: Vec<&[u8]> = buffer[pos..end_pos].splitn(2, |&b| b == b'=').collect();
            if parts.len() == 2 {
                let key = String::from_utf8_lossy(parts[0]).into_owned();
                // let value = String::from_utf8_lossy(&parts[1][..parts[1].len()-1]).into_owned();
                let entry = LogEntry { pos: pos as u64, len: (end_pos - pos) as u64 };
                self.index.insert(key, entry);
            }
            //get pos ready for next log entry
            pos = end_pos + 1;
        }
        Ok(())
    }

    
    /**
     * Writes a new key-value pair to the log file and updates the in-memory index and cache.
     * This function appends the new entry at the end of the log file, ensuring that all writes
     * are sequential for efficiency. The in-memory index is then updated with the position and
     * length of the new entry, allowing for quick future lookups. Additionally, the key-value
     * pair is stored in the LRU cache to speed up read operations.
     * 
     * # Parameters
     * - `key`: A `String` representing the key of the entry to be added or updated in the log file.
     * - `value`: A `String` representing the value associated with the key.
     * 
     * # Behavior
     * - The function seeks to the end of the log file to ensure that the new entry is appended,
     *   preserving the order of writes.
     * - A new log entry is formatted as "key=value\n" and written to the file.
     * - The in-memory index is updated with the new entry's position and length, keyed by the entry's key.
     * - The key-value pair is also inserted into the cache, with any existing entry for the key being
     *   overwritten to reflect the most current value.
     * 
     * # Error Handling
     * - If there is an error while seeking to the end of the file, writing the entry, or updating
     *   the index or cache, an `io::Error` is returned.
     * 
     * # Returns
     * - `Ok(())` if the entry is successfully written to the log file, and the index and cache are updated.
     * - `Err(io::Error)` if an error occurs during any part of the process.
     * 
     * Note: This function ensures that the KeyValueStore remains consistent by atomically
     * updating the log file, index, and cache. However, callers should handle possible I/O errors,
     * especially when dealing with file system limitations or write failures.
     */
    async fn set(&mut self, key: String, value: String) -> io::Result<()> {
        //move the pointer in the log to the end of the latest log entry
        let pos = self.log.seek(io::SeekFrom::End(0)).await?;
        let entry = format!("{}={}\n", key, value);
        self.log.write_all(entry.as_bytes()).await?;
        self.index.insert(key.clone(), LogEntry { pos, len: entry.len() as u64 });

        // Update the cache with the new value
        let mut cache_lock = self.cache.lock().await;
        cache_lock.put(key, Some(value));

        Ok(())
    }

    /**
     * The GET utilizes an in-memory cache and checks it for the value, and if not found,
     * it uses the index to jump directly to the last known position of a key-value 
     * pair, which should ensure it retrieves the latest value for a given key even in cases
     * we have multiple entries (lines) for the same key.
     * 
     * # Parameters
     * - `key`: A string slice (`&str`) representing the key for which the value is being retrieved.
     * 
     * # Behavior
     * - The cache is checked first to minimize disk I/O and improve retrieval speed. If the value
     *   is present in the cache, it is returned immediately.
     * - If the value is not found in the cache, the function then looks up the key in the index
     *   to find the position and length of the log entry.
     * - The log file is read at the specified position to extract the value. This operation involves
     *   seeking to the correct position in the file and reading the specified number of bytes into a buffer.
     * - The key-value pair is parsed from the buffer, and the value is cached before being returned.
     * 
     * # Error Handling
     * - If the key is not found in either the cache or the index, `Ok(None)` is returned, indicating
     *   that there is no value associated with the key.
     * - If any I/O errors occur while reading from the log file, or if the log entry format is invalid,
     *   an `Err(io::Error)` is returned with the appropriate error.
     * 
     * # Returns
     * - `Ok(Some(String))` if the value is found, containing the value associated with the key.
     * - `Ok(None)` if the key is not found in the cache or the index.
     * - `Err(io::Error)` if an error occurs during the operation.
     * 
     * Note: This function emphasizes efficiency by utilizing an LRU cache for frequently accessed values.
     * However, it incurs overhead from locking the cache for each access, which is a trade-off for
     * maintaining cache consistency in asynchronous environments. To minimize this overhead, we can
     * fine-tune the cache size and/or explore lock-free caching mechanisms if the performance impact
     * becomes significant.
     */

    async fn get(&mut self, key: &str) -> io::Result<Option<String>> {
        //first, lets check the cache for the value (note, this might cause overhead due to locking)
        let mut cache_lock = self.cache.lock().await;
        if let Some(value) = cache_lock.get(key) {
            return Ok(value.clone());
        }
        drop(cache_lock); //release the lock before proceeding

        //Now, since val is not in cache, check index hashmap for key before calling log and then cache the result
        if let Some(entry) = self.index.get(key) {
            let mut buffer = vec![0; entry.len as usize];
            //using entry.pos to move the pointer in memory to the exact location
            self.log.seek(io::SeekFrom::Start(entry.pos)).await?;
            //read into the buffer with seek
            self.log.read_exact(&mut buffer).await?;
            
            // Assuming `buffer` is a &[u8] that represents "key=value\n"
            let key_value_pair: Vec<&[u8]> = buffer.splitn(2, |&b| b == b'=').collect();

            //throw error is the key_value pair is not as expected for the KV store
            if key_value_pair.len() != 2 {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid key-value pair format"));
            }
            //Extract the value
            let value = match key_value_pair[1].last() {
                Some(&last_byte) if last_byte == b'\n' => {
                    //extracting the value without newline char at the end
                    String::from_utf8_lossy(&key_value_pair[1][..key_value_pair[1].len() - 1]).to_string()
                },
                _ => String::from_utf8_lossy(key_value_pair[1]).to_string()
            };

            //cache before returning the result
            let mut cache_lock = self.cache.lock().await;
            //automatic release of our cache lock when the lock guard goes out of scope
            cache_lock.put(key.to_string(), Some(value.clone()));
            return Ok(Some(value))
        }
        Ok(None)
    }

    async fn delete(&mut self, key: &str) -> io::Result<()> {
        if self.index.remove(key).is_some() {
            //appending a new "deleted" marker for the key
            self.set(key.to_string(), "deleted".to_string()).await?;
        }
        // Reflect the change in the cache by setting the value to None for persistency
        let mut cache_lock = self.cache.lock().await;
        cache_lock.put(key.to_string(), None);
        Ok(())
    }

    /**
     * Compact function can be called on a scheduled basis in order to write
     * to a new copy of store all the non-deleted values.
     */
    async fn compact(&mut self) -> io::Result<()> {
        // Temporary file path for the new compacted log
        let temp_path = format!("{}.compact", self.log_path);
        let mut temp_file: File = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&temp_path)
            .await?;

        //add to the new KV index as we go through old index
        let mut new_index: HashMap<String, LogEntry> = HashMap::new();
        let mut new_pos = 0u64;
    
        // Re-read the original log, and for each key, write the latest value to the temp file,
        // skipping deleted entries or older versions of updated entries.
        // cloning self.index before the call to self.get() to meet the borrowing rules
        let keys: Vec<_> = self.index.keys().cloned().collect();
        for key in keys {
            if let Some(value) = self.get(&key).await? {
                // Assuming `get` fetches the latest non-deleted value for the key
                if value != "deleted" { // Check if not marked as deleted
                    let new_line = format!("{}={}\n", key, value);
                    temp_file.write_all(new_line.as_bytes()).await?;
                    // Update the new index with the new position and length
                    new_index.insert(key.clone(), LogEntry { pos: new_pos, len: new_line.len() as u64 });
                    new_pos += new_line.len() as u64; // Update new_pos for the next entry
                }
            }
        }
    
        //ensure flushing for data integrity (although Drop handles closure)
        temp_file.flush().await?;
        drop(temp_file); // Close the file before renaming
        fs::rename(&temp_path, &self.log_path).await?;

        // Update the log file handle to the new file
        self.log = OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .open(&self.log_path)
            .await?;
        self.index = new_index;

        Ok(())
    }
    
}


#[tokio::main]
async fn main() -> io::Result<()> {
    let mut store = KeyValueStore::new("dilnoza_store.log").await?;

    store.set("key1".to_string(), "value1".to_string()).await?;
    println!("{:?}", store.get("key1").await?);
    store.set("key2".to_string(), "value2".to_string()).await?;
    println!("{:?}", store.get("key2").await?);
    store.set("key3".to_string(), "value3".to_string()).await?;
    println!("{:?}", store.get("key3").await?);
    store.set("key4".to_string(), "value4".to_string()).await?;
    println!("{:?}", store.get("key4").await?);
    store.delete("key1").await?;
    println!("{:?}", store.get("key1").await?);

    store.compact().await?;
    Ok(())
}