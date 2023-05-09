//! # Amandine
//! Amandine is a tiny json database for rust. It is designed to be used in small, embedded, or
//! client-side projects.
//! ## Usage
//! ```rust
//! use amandine::{Data, Database};
//! use serde::{Serialize, Deserialize};
//! use std::fs;
//! use std::path::Path;
//!
//! #[derive(Serialize, Deserialize, Debug, Clone, Data)]
//! struct User {
//!     name: String,
//!     age: u8,
//! }
//!
//! fn main() {
//!     let db = Database::new();
//!     let dbPath = Path::new("./db"); // should be folder/dir path
//!     db.connect(dbPath.to_path_buf()).unwrap();
//!     db.create_collection("users").unwrap(); // create a collection to store data
//!     let user = User {
//!         name: "John".to_string(),
//!         age: 20,
//!     }
//!     db.insert("users", user).unwrap(); // insert data into collection
//! }

pub mod db;
pub mod error;

pub use db::Data;
pub use db::Database;
