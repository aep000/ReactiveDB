use reactivedb_rust_client::types::DBResponse;
use reactivedb_rust_client::types::DBRequest;
use reactivedb_rust_client::types::EntryBuilder;
use reactivedb_rust_client::types::EntryValue;
use reactivedb_rust_client::types::ListenEvent;
use reactivedb_rust_client::client::Client;
use std::{thread, time};
#[tokio::main]
async fn main() {
    
    let mut client = Client::new("127.0.0.1:1108");
    client.open_connection().await.unwrap();
    client.subscribe_to_event("users".to_string(), ListenEvent::Insert, Box::new(|resp: DBResponse| -> Result<(), ()> {
        println!("\nEVENT: {:?}\n", resp);
        Ok(())
    })).await.unwrap();
    let mut entry_to_insert = EntryBuilder::new();
    entry_to_insert.column("age", EntryValue::Integer(22));
    entry_to_insert.column("name", EntryValue::Str("Alex".to_string()));
    client.make_request(DBRequest::new_insert("users".to_string(), entry_to_insert.build())).await.unwrap();

    let mut entry_to_insert = EntryBuilder::new();
    entry_to_insert.column("grade", EntryValue::Integer(95));
    entry_to_insert.column("name", EntryValue::Str("Alex".to_string()));
    client.make_request(DBRequest::new_insert("grades".to_string(), entry_to_insert.build())).await.unwrap();
        let result = client.make_request(
            DBRequest::new_find_one(
                "unionTest".to_string(), 
                "matchingKey".to_string(), 
                EntryValue::Str("Alex".to_string())
            )).await.unwrap();
        println!("{:?}", result);

    let ten_millis = time::Duration::from_millis(100);    
    thread::sleep(ten_millis);
}