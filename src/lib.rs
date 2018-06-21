#[macro_use(bson, doc)]
extern crate bson;
extern crate mongodb;

use bson::Bson;
use mongodb::{Client, ThreadedClient};
use mongodb::db::ThreadedDatabase;

static mut db : ThreadedDatabase;

#[no_mangle]
pub extern fn init_db() {
    let client = Client::with_uri("mongodb://HDF5MetadataTest_admin:ekekek19294jdwss2k@mongodb03.nersc.gov/HDF5MetadataTest")
    .expect("Failed on connection");
    db = client.db("HDF5MetadataTest");
    db.auth("HDF5MetadataTest_admin","ekekek19294jdwss2k").unwrap();
}

#[no_mangle]
pub extern fn random_test() {
    for n in (1..4).rev() {
        println!("Hello,my ssss world! {}", n);
    }
    let coll = db.collection("abcde");

    coll.insert_one(doc!{ "title" => "Back to the Future" }, None).unwrap();
    let doc = doc! {
        "title": "Jaws",
        "array": [ 1, 2, 3 ],
    };
        // Insert document into 'test.movies' collection

    coll.insert_one(doc.clone(), None)
        .ok().expect("Failed to insert document.");

    // Find the document and receive a cursor
    let mut cursor = coll.find(Some(doc.clone()), None)
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
