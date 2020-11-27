use rand::Rng;
use reactivedb_rust_client::client::Client;
use reactivedb_rust_client::types::DBRequest;
use reactivedb_rust_client::types::EntryBuilder;
use reactivedb_rust_client::types::EntryValue;
use std::time::Instant;

#[tokio::main]
async fn main() {
    let start = Instant::now();
    insert().await;
    println!("Insert 1000 entries took {}", start.elapsed().as_millis());
    let start = Instant::now();
    get_all().await;
    println!("Search 1000 entries took {}", start.elapsed().as_millis());
}

async fn insert() {
    let mut client = Client::new("127.0.0.1:1108");
    client.open_connection().await.unwrap();
    let arr = 0..1000;
    let mut rng = rand::thread_rng();
    for n in arr {
        let mut entry_to_insert = EntryBuilder::new();
        let i = rng.gen_range(0.0, 10.0) as isize;
        entry_to_insert.column("testForIteration", EntryValue::Integer(n));
        entry_to_insert.column("testForIndex", EntryValue::Integer(i));
        let request = DBRequest::new_insert("testTable".to_string(), entry_to_insert.build());
        client.make_request(request).await.unwrap();
    }
}

async fn get_all() {
    let arr = 0..1000;
    for n in arr {
        let mut client = Client::new("127.0.0.1:1108");
        client.open_connection().await.unwrap();
        let find_one_request = DBRequest::new_find_one(
            "testTable".to_string(),
            "testForIteration".to_string(),
            EntryValue::Integer(n),
        );
        println!("{:?}",client.make_request(find_one_request).await.unwrap());
    }
}
