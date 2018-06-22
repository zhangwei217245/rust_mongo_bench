#[macro_use(bson, doc)]
extern crate bson;
extern crate mongodb;
extern crate serde_json;


#[macro_use]
extern crate lazy_static;
extern crate libc;


use std::ffi::CStr;
use libc::c_char;
use bson::Bson;
use mongodb::{Client, ThreadedClient};
use mongodb::db::{ThreadedDatabase};
use mongodb::coll::Collection;
use serde_json::Value;

// lazy_static! {
//     static ref MONGO_DB: Database = {
//         let client = Client::with_uri("mongodb://HDF5MetadataTest_admin:ekekek19294jdwss2k@mongodb03.nersc.gov/HDF5MetadataTest")
//         .expect("Failed on connection");
//         let db = client.db("HDF5MetadataTest");
//         db.auth("HDF5MetadataTest_admin","ekekek19294jdwss2k").unwrap();
//         //dbMap.insert(String::from("db"), &db);
//         db
//     };
// }

lazy_static! {
    static ref MONGO_COLL: Collection = {
        let client = Client::with_uri("mongodb://HDF5MetadataTest_admin:ekekek19294jdwss2k@mongodb03.nersc.gov/HDF5MetadataTest")
        .expect("Failed on connection");
        client.add_completion_hook(log_query_duration).unwrap();
        let db = client.db("HDF5MetadataTest");
        db.auth("HDF5MetadataTest_admin","ekekek19294jdwss2k").unwrap();
        db.collection("abcde")
    };
}

fn log_query_duration(client: Client, command_result: &CommandResult) {
    match command_result {
        &CommandResult::Success { duration, .. } => {
            println!("Command took {} nanoseconds.", duration);
        },
        _ => println!("Failed to execute command."),
    }
}

#[no_mangle]
pub extern fn init_db() -> i64 {
    let query_doc = doc!{};
    let db_count = MONGO_COLL.count(Some(query_doc), None).unwrap();
    println!("db count = {}", db_count);
    db_count
}

#[no_mangle]
pub extern "C" fn importing_json_doc_to_db (json_str: *const c_char) -> i64 {
    let c_str = unsafe {
        assert!(!json_str.is_null());
        CStr::from_ptr(json_str)
    };
    let r_str = c_str.to_str().unwrap().to_owned();
    // let string_count = r_str.len() as i32;
    let json : Value = serde_json::from_str(&r_str).unwrap();
    let bson : Bson = json.into();
    let doc = bson.as_document().unwrap();
    for number in (0..1000000) { // inserting 1M documents. 
        MONGO_COLL.insert_one(doc.clone(), None)
        .ok().expect("Failed to insert document.");
    }
    
    let db_count = MONGO_COLL.count(Some(doc!{}), None).unwrap();
    println!("db count = {}", db_count);
    db_count
}

#[no_mangle]
pub extern fn random_test() {
    for n in (1..4).rev() {
        println!("Hello,my ssss world! {}", n);
    }

    MONGO_COLL.insert_one(doc!{ "title" => "Back to the Future" }, None).unwrap();
    let doc = doc! {
        "title": "Jaws",
        "array": [ 1, 2, 3 ],
    };
        // Insert document into 'test.movies' collection

    MONGO_COLL.insert_one(doc.clone(), None)
        .ok().expect("Failed to insert document.");

    // Find the document and receive a cursor
    let mut cursor = MONGO_COLL.find(Some(doc.clone()), None)
        .ok().expect("Failed to execute find.");

    let item = cursor.next();

    // cursor.next() returns an Option<Result<Document>>
    match item {
        Some(Ok(doc)) => match doc.get("title") {
            Some(&Bson::String(ref title)) => println!("{}", title),
            _ => panic!("Expected title to be a string!"),
        },
        Some(Err(_)) => panic!("Failed to get next from server!"),
        None => panic!("Server returned no results!"),
    } 
}
