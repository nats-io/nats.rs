use nats;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct Person {
    first_name: String,
    last_name: String,
    age: u8,
}

fn main() -> std::io::Result<()> {
    let nc = nats::connect("demo.nats.io")?;
    let subj = nc.new_inbox();

    let p = Person {
        first_name: "derek".to_owned(),
        last_name: "collison".to_owned(),
        age: 22,
    };

    let sub = nc.subscribe(&subj)?;
    nc.publish(&subj, serde_json::to_vec(&p)?)?;

    let mut p2 = sub.iter().map(move |msg| {
        let p: Person = serde_json::from_slice(&msg.data).unwrap();
        p
    });
    println!("received {:?}", p2.next().unwrap());

    Ok(())
}
