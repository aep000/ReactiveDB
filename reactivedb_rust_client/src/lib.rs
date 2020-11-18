pub mod types;
pub mod client;
#[cfg(test)]
mod tests {
    use crate::types::DBResponse;
use crate::types::EntryValue;
    use crate::types::EntryBuilder;
    use crate::types::DBRequest;
    use crate::client::Client;
    #[test]
    fn test_insert() {
        let mut client = Client::new("127.0.0.1:1108");
        client.open_connection().unwrap();
        let mut entry_builder = EntryBuilder::new();
        entry_builder.column("testForIndex", EntryValue::Integer(11));
        entry_builder.column("testForIteration", EntryValue::Integer(11));

        let insert_request = DBRequest::new_insert("testTable".to_string(), entry_builder.build());
        print!("\n{:?}\n", insert_request);
        let response = client.make_request(insert_request).unwrap();
        assert_eq!(DBResponse::NoResult(Ok(())), response);
    }

    #[test]
    fn test_find_one() {
        let mut client = Client::new("127.0.0.1:1108");
        client.open_connection().unwrap();
        let mut entry_builder = EntryBuilder::new();
        entry_builder.column("testForIndex", EntryValue::Integer(11));
        entry_builder.column("testForIteration", EntryValue::Integer(11));

        let insert_request = DBRequest::new_find_one("testTable".to_string(), "testForIndex".to_string(), EntryValue::Integer(11));
        print!("\n{:?}\n", insert_request);
        let response = client.make_request(insert_request).unwrap();
        match response {
            DBResponse::OneResult(response) => {
                let entry = response.unwrap().unwrap();
                assert_eq!(&EntryValue::Integer(11), entry.get("testForIndex").unwrap());
                assert_eq!(&EntryValue::Integer(11), entry.get("testForIteration").unwrap());
            },
            _ => panic!("Too many responses")

        }
    }
}


