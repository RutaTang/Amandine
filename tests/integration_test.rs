use amandine::{
    self,
    db::{Data, Database, TDatabase},
};
use serde::{Deserialize, Serialize};

#[test]
fn integration_single_collection() {
    let temp_dir = tempfile::tempdir().unwrap();

    // Create a new database
    let mut db = Database::new();
    let r = db.connect(temp_dir.path().to_path_buf());
    assert!(r.is_ok());

    // Create a new collection
    let r = db.create_collection("test");
    assert!(r.is_ok());

    // Define a data structure
    #[derive(Serialize, Deserialize, Debug, Clone)]
    struct TestData {
        name: String,
        age: u8,
    }
    impl Data for TestData {
        fn uuid(&self) -> String {
            self.name.clone()
        }
    }

    // Insert data into the collection
    let test_data_01 = TestData {
        name: "John".to_string(),
        age: 42,
    };
    let test_data_02 = TestData {
        name: "Jane".to_string(),
        age: 24,
    };
    let test_data_03 = TestData {
        name: "Jack".to_string(),
        age: 36,
    };
    let r = db.insert_data("test", test_data_01.clone());
    assert!(r.is_ok());
    let r = db.insert_data("test", test_data_02.clone());
    assert!(r.is_ok());
    let r = db.insert_data("test", test_data_03.clone());
    assert!(r.is_ok());

    let r = db.list_data::<TestData>("test");
    assert!(r.is_ok());
    assert_eq!(r.unwrap().len(),3);

    // Update data
    let update_data = TestData{
        name: test_data_02.name.clone(),
        age: 43,
    };
    let r = db.update_data("test", update_data.clone());
    assert!(r.is_ok());

    let query_data = db.query_data::<TestData>("test", &test_data_02.name);
    assert!(query_data.is_ok());
    let query_data = query_data.unwrap();
    assert_eq!(query_data.name, update_data.name);


    // Delete data
    let r = db.delete_data::<TestData>("test", &test_data_01.name);
    assert!(r.is_ok());
    let r = db.delete_data::<TestData>("test", &test_data_02.name);
    assert!(r.is_ok());

    let query_data = db.query_data::<TestData>("test", &test_data_01.name);
    assert!(query_data.is_err());
    let query_data = db.query_data::<TestData>("test", &test_data_02.name);
    assert!(query_data.is_err());
    let query_data = db.query_data::<TestData>("test", &test_data_03.name);
    assert!(query_data.is_ok());

    let r = db.list_data::<TestData>("test");
    assert!(r.is_ok());
    assert_eq!(r.unwrap().len(),1);
}
