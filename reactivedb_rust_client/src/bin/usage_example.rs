use reactivedb_rust_client::types::DBRequest;
use reactivedb_rust_client::types::EntryBuilder;
use reactivedb_rust_client::types::EntryValue;
use reactivedb_rust_client::client::Client;

fn main() {
    let mut client = Client::new("127.0.0.1:1108");
    client.open_connection().unwrap();
    let mut entry_to_insert = EntryBuilder::new();
    entry_to_insert.column("age", EntryValue::Integer(22));
    entry_to_insert.column("name", EntryValue::Str("Alex".to_string()));
    client.make_request(DBRequest::new_insert("users".to_string(), entry_to_insert.build())).unwrap();

    let mut entry_to_insert = EntryBuilder::new();
    entry_to_insert.column("grade", EntryValue::Integer(95));
    entry_to_insert.column("name", EntryValue::Str("Alex".to_string()));
    client.make_request(DBRequest::new_insert("grades".to_string(), entry_to_insert.build())).unwrap();

    let result = client.make_request(
        DBRequest::new_find_one(
            "unionTest".to_string(), 
            "matchingKey".to_string(), 
            EntryValue::Str("Alex".to_string())
        )).unwrap();
    println!("{:?}", result);

}