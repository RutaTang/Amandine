use serde::de::DeserializeOwned;
use serde::{self, Serialize};
use std::fs;
use std::path::PathBuf;

use crate::error::DBError;

/// Trait for data types that can be stored in the database, users must implement this trait for their data types
pub trait Data: Serialize + DeserializeOwned + Clone {
    fn uuid(&self) -> String;
}

/// Trait for database types, [Database] implements this trait
pub trait TDatabase {
    fn connect(&mut self, path: PathBuf) -> Result<(), DBError>;
    fn create_collection(&self, name: &str) -> Result<(), DBError>;
    fn list_collections(&self) -> Result<Vec<String>, DBError>;
    fn delete_collection(&self, name: &str) -> Result<(), DBError>;
    fn rename_collection(&self, name: &str, new_name: &str) -> Result<(), DBError>;
    fn insert<T: Data>(&self, collection: &str, data: T) -> Result<(), DBError>;
    fn query<T: Data>(&mut self, collection: &str, uuid: &str) -> Result<T, DBError>;
    fn update<T: Data>(&mut self, collection: &str, data: T) -> Result<(), DBError>;
    fn delete<T: Data>(&mut self, collection: &str, uuid: &str) -> Result<(), DBError>;
    fn list<T: Data>(&self, collection: &str) -> Result<Vec<T>, DBError>;
}

/// Database struct used to interact with the database
pub struct Database {
    path: PathBuf,
}

impl Database {
    /// Creates a new database instance
    pub fn new() -> Database {
        Database {
            path: PathBuf::new(),
        }
    }

    /// Reads a collection from the database
    fn read_collection<T: Data>(&self, collection: &str) -> Result<Vec<T>, DBError> {
        // find collection file
        let mut collection = collection.to_lowercase();
        collection.push_str(".json");
        let collection_path = self.path.join(collection);
        if !collection_path.exists() {
            return Result::Err(DBError("Collection does not exist"));
        }
        // read collection file
        let r = fs::read_to_string(&collection_path);
        if r.is_err() {
            return Result::Err(DBError("Could not read collection"));
        }
        let r = r.unwrap();
        let collection_data: Vec<T> = serde_json::from_str(&r).unwrap();
        Result::Ok(collection_data)
    }

    /// Writes data to a collection in the database
    fn write_collection<T: Data>(&self, collection: &str, data: Vec<T>) -> Result<(), DBError> {
        // find collection file
        let mut collection = collection.to_lowercase();
        collection.push_str(".json");
        let collection_path = self.path.join(collection);
        if !collection_path.exists() {
            return Result::Err(DBError("Collection does not exist"));
        }
        // write collection file
        let w = fs::write(collection_path, serde_json::to_string(&data).unwrap());
        if w.is_err() {
            return Result::Err(DBError("Could not write collection"));
        }
        Result::Ok(())
    }
}

impl TDatabase for Database {
    /// Connects to the database, creates the database if it does not exist
    /// # Arguments
    /// * `path` - Path to the database folder
    fn connect(&mut self, path: PathBuf) -> Result<(), DBError> {
        // check existence of folder path
        if path.exists() {
            // check if path is a directory
            if !path.is_dir() {
                return Result::Err(DBError("Path is not a directory"));
            }
        } else {
            let r = fs::create_dir_all(&path);
            if r.is_err() {
                return Result::Err(DBError("Could not create directory"));
            }
        }
        self.path = path;
        Result::Ok(())
    }

    /// Creates a new collection in the database
    fn create_collection(&self, name: &str) -> Result<(), DBError> {
        let mut name = name.to_lowercase();
        name.push_str(".json");
        // check if collection exists
        let collection_path = self.path.join(name);
        if collection_path.exists() {
            return Result::Err(DBError("Collection already exists"));
        }
        // create collection
        let r = fs::write(collection_path, "[]");
        if r.is_err() {
            print!("{}", r.err().unwrap());
            return Result::Err(DBError("Could not create collection"));
        }
        Result::Ok(())
    }

    /// Lists collection in the database
    fn list_collections(&self) -> Result<Vec<String>, DBError> {
        self.path
            .read_dir()
            .map(|r| {
                r.filter_map(|r| r.ok())
                    .filter(|r| r.path().is_file())
                    .filter(|r| r.path().extension().unwrap_or_default() == "json")
                    .map(|r| r.path().file_stem().unwrap().to_str().unwrap().to_string())
                    .collect()
            })
            .map_err(|_| DBError("Could not list collections"))
    }

    /// Deletes a collection from the database
    fn delete_collection(&self, name: &str) -> Result<(), DBError> {
        let mut name = name.to_lowercase();
        name.push_str(".json");
        let collection_path = self.path.join(name);
        if !collection_path.exists() {
            return Result::Err(DBError("Collection does not exist"));
        }
        let r = fs::remove_file(collection_path);
        if r.is_err() {
            return Result::Err(DBError("Could not delete collection"));
        }
        Result::Ok(())
    }

    /// Inserts data into a collection in the database
    fn insert<T: Data>(&self, collection: &str, data: T) -> Result<(), DBError> {
        let mut c: Vec<T> = self.read_collection(collection)?;
        for i in &c {
            if i.uuid() == data.uuid() {
                return Result::Err(DBError("Data already exists"));
            }
        }
        c.push(data);
        self.write_collection(collection, c)?;
        Result::Ok(())
    }

    /// Queries data from a collection in the database
    fn query<T: Data>(&mut self, collection: &str, uuid: &str) -> Result<T, DBError> {
        let c: Vec<T> = self.read_collection(collection)?;
        for i in &c {
            if i.uuid() == uuid {
                return Result::Ok(i.clone());
            }
        }
        Result::Err(DBError("Data not found"))
    }

    /// Updates data in a collection in the database
    fn update<T: Data>(&mut self, collection: &str, data: T) -> Result<(), DBError> {
        let mut c: Vec<T> = self.read_collection(collection)?;
        for i in 0..c.len() {
            if c[i].uuid() == data.uuid() {
                c[i] = data;
                self.write_collection(collection, c)?;
                return Result::Ok(());
            }
        }
        Result::Err(DBError("Data not found"))
    }

    /// Deletes data from a collection in the database
    fn delete<T: Data>(&mut self, collection: &str, uuid: &str) -> Result<(), DBError> {
        let mut c: Vec<T> = self.read_collection(collection)?;
        for i in 0..c.len() {
            if c[i].uuid() == uuid {
                c.remove(i);
                self.write_collection(collection, c)?;
                return Result::Ok(());
            }
        }
        Result::Err(DBError("Data not found"))
    }

    /// Lists data from a collection in the database
    fn list<T: Data>(&self, collection: &str) -> Result<Vec<T>, DBError> {
        self.read_collection(collection)
    }

    /// Updates the name of a collection in the database
    fn rename_collection(&self, name: &str, new_name: &str) -> Result<(), DBError> {
        let mut name = name.to_lowercase();
        name.push_str(".json");
        let mut new_name = new_name.to_lowercase();
        new_name.push_str(".json");
        let collection_path = self.path.join(name);
        let new_collection_path = self.path.join(new_name);
        if !collection_path.exists() {
            return Result::Err(DBError("Collection does not exist"));
        }
        if new_collection_path.exists() {
            return Result::Err(DBError("Collection already exists"));
        }
        let r = fs::rename(collection_path, new_collection_path);
        if r.is_err() {
            return Result::Err(DBError("Could not rename collection"));
        }
        Result::Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serde::Deserialize;
    use tempfile::{tempdir, TempDir};

    fn setup() -> (Database, TempDir) {
        let mut db = Database::new();
        let db_dir = tempdir().unwrap();
        db.connect(db_dir.path().to_path_buf()).unwrap();
        (db, db_dir)
    }

    #[test]
    fn test_connect() {
        let (_db, db_dir) = setup();
        assert!(db_dir.path().exists());
        assert!(db_dir.path().is_dir());
    }

    #[test]
    fn test_create_collection() {
        let (db, _db_dir) = setup();
        db.create_collection("test").unwrap();
        assert!(db.path.join("test.json").exists());
        assert!(db.path.join("test.json").is_file());
    }

    #[test]
    fn test_insert_data() {
        #[derive(Debug, Serialize, Deserialize, Clone)]
        struct TestData {
            uuid: String,
            name: String,
        }
        impl Data for TestData {
            fn uuid(&self) -> String {
                self.uuid.clone()
            }
        }
        let (db, _db_dir) = setup();
        db.create_collection("test").unwrap();
        let data = TestData {
            uuid: "test".to_string(),
            name: "test".to_string(),
        };
        db.insert("test", data.clone()).unwrap();
        let r: Vec<TestData> = db.read_collection("test").unwrap();
        assert_eq!(r[0].uuid, data.uuid);
    }

    #[test]
    fn test_query_data() {
        #[derive(Debug, Serialize, Deserialize, Clone)]
        struct TestData {
            uuid: String,
            name: String,
        }
        impl Data for TestData {
            fn uuid(&self) -> String {
                self.uuid.clone()
            }
        }
        let (mut db, _db_dir) = setup();
        db.create_collection("test").unwrap();
        let data_1 = TestData {
            uuid: "test".to_string(),
            name: "test".to_string(),
        };
        db.insert("test", data_1.clone()).unwrap();
        let data_2 = TestData {
            uuid: "test2".to_string(),
            name: "test2".to_string(),
        };
        db.insert("test", data_2.clone()).unwrap();
        let r: TestData = db.query("test", "test").unwrap();
        assert_eq!(r.uuid, data_1.uuid);
        assert_eq!(r.name, data_1.name);
    }

    #[test]
    fn test_delete_data() {
        #[derive(Debug, Serialize, Deserialize, Clone)]
        struct TestData {
            uuid: String,
            name: String,
        }
        impl Data for TestData {
            fn uuid(&self) -> String {
                self.uuid.clone()
            }
        }
        let (mut db, _db_dir) = setup();
        db.create_collection("test").unwrap();
        let data_1 = TestData {
            uuid: "test".to_string(),
            name: "test".to_string(),
        };
        db.insert("test", data_1.clone()).unwrap();
        let data_2 = TestData {
            uuid: "test2".to_string(),
            name: "test2".to_string(),
        };
        db.insert("test", data_2.clone()).unwrap();
        db.delete::<TestData>("test", "test").unwrap();
        let r: Vec<TestData> = db.read_collection("test").unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].uuid, data_2.uuid);
    }

    #[test]
    fn test_update_data() {
        #[derive(Debug, Serialize, Deserialize, Clone)]
        struct TestData {
            uuid: String,
            name: String,
        }
        impl Data for TestData {
            fn uuid(&self) -> String {
                self.uuid.clone()
            }
        }
        let (mut db, _db_dir) = setup();
        db.create_collection("test").unwrap();
        let data_1 = TestData {
            uuid: "test".to_string(),
            name: "test".to_string(),
        };
        db.insert("test", data_1.clone()).unwrap();
        let data_2 = TestData {
            uuid: "test".to_string(),
            name: "test2".to_string(),
        };
        db.update("test", data_2.clone()).unwrap();
        let r: Vec<TestData> = db.read_collection("test").unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].uuid, data_2.uuid);
        assert_eq!(r[0].name, data_2.name);
    }

    #[test]
    fn test_list_collections(){
        let (db, _db_dir) = setup();
        db.create_collection("test").unwrap();
        db.create_collection("test2").unwrap();
        let r: Vec<String> = db.list_collections().unwrap();
        assert_eq!(r.len(), 2);
    }

    #[test]
    fn test_delete_collection(){
        let (db, _db_dir) = setup();
        db.create_collection("test").unwrap();
        db.delete_collection("test").unwrap();
        assert!(!db.path.join("test.json").exists());
    }

    #[test]
    fn test_rename_collection(){
        let (db, _db_dir) = setup();
        db.create_collection("test").unwrap();
        db.rename_collection("test", "test2").unwrap();
        assert!(!db.path.join("test.json").exists());
        assert!(db.path.join("test2.json").exists());
    }
}
