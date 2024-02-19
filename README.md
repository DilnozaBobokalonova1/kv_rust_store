# Key-Value Store Implementation

## Overview

This project implements a simple yet efficient key-value store in Rust, leveraging asynchronous I/O for high performance and scalability. The design focuses on durability, efficiency, and ease of use, incorporating several key features to optimize read/write operations and ensure data persistence.

## Key Features

- **Append-Only Log File:** Utilizes an append-only log file for data persistence, ensuring durability and simplification of the write path.
- **In-Memory Indexing:** Implements an in-memory `HashMap` to quickly locate data in the log file without full scans, significantly speeding up read operations.
- **Log Compaction:** Supports log compaction to remove deleted or superseded entries, optimizing storage utilization and maintaining performance over time.
- **LRU Caching:** Integrates an LRU (Least Recently Used) caching mechanism to reduce disk I/O for frequently accessed data, further enhancing read performance.

## Design Considerations

- **SSD Wear Minimization:** Recognizing the wear-out mechanism of SSDs, the store checks for existing data before writing to the disk, reducing unnecessary write operations.
- **Batch Writes:** Facilitates batch writes, leveraging the efficiency of SSDs in handling larger, less frequent writes compared to multiple small, frequent ones. This strategy extends the SSD's lifespan and improves write efficiency.
- **Write Buffering:** Offers optional write buffering in RAM, allowing aggregation and optimization of write operations before they are committed to disk. This approach ensures efficient use of write cycles, writing only final or necessary data to the SSD.

## Implementation Details

- **LogEntry Struct:** Represents each entry's position and length within the log file, facilitating quick data retrieval without file scanning.
- **KeyValueStore Struct:** Manages the key-value store, including the append-only log, in-memory index, file path, and cache. Provides asynchronous CRUD operations, leveraging Tokio for non-blocking I/O.

## Asynchronous Operations

All read, write, delete, and compaction operations are asynchronous, ensuring the system remains responsive and scalable under load.

## Usage

The store is initialized with a file path to the log file and supports asynchronous set, get, delete, and compaction operations. It is suitable for applications requiring efficient data persistence and access with a simple key-value model.

## Future Improvements

- Implementing more sophisticated data validation and error handling for robustness.
- Exploring additional caching strategies or lock-free mechanisms to further enhance performance.
- Evaluating and integrating more advanced storage formats or indexing techniques for scalability.
- Adding snapshot functionality since we operate on single node which is a single point of failure. Can run snapshot every day to ensure data is copied and saved in case of failure. Could look something like this:
```rust
struct KeyValueStore {
    // Existing fields...
    log: Mutex<File>,
    // Other fields...
}

impl KeyValueStore {
    // Existing methods...

    /// Creates a snapshot of the current state of the KeyValueStore's log file and saves it to a specified file.
    ///
    /// # Parameters
    ///
    /// - `snapshot_path`: A string slice (`&str`) representing the path where the snapshot will be saved.
    ///
    /// # Returns
    ///
    /// An `io::Result<()>` indicating success or failure of the snapshot creation.
    ///
    /// # Errors
    ///
    /// This method can return an error if there is a problem with file I/O operations
    /// or if writing the snapshot fails.
    pub async fn create_snapshot(&self, snapshot_path: &str) -> Result<()> {
        // Obtain a lock on the log file to ensure exclusive access
        let mut log_file = self.log.lock().await?;

        // Create a new file to write the snapshot
        let mut snapshot_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(snapshot_path)
            .await?;

        // Copy the contents of the log file to the snapshot file
        io::copy(&mut log_file, &mut snapshot_file).await?;

        // Ensure data is written to disk before exiting
        snapshot_file.flush().await?;

        Ok(())
    }
}
```
