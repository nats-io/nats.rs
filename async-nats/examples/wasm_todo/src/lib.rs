use leptos::{prelude::*, task::spawn_local};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
struct Todo {
    id: usize,
    title: String,
    completed: bool,
}

#[component]
pub fn App() -> impl IntoView {
    use async_nats::jetstream;
    use futures::{channel::mpsc, StreamExt};

    // let (tx, mut rx) = mpsc::channel(1);
    let todos = RwSignal::new(vec![]);
    spawn_local(async move {
        let client = async_nats::connect("ws://localhost:8444").await.unwrap();
        let jetstream = jetstream::new(client);

        let kv = jetstream
            .create_or_update_key_value(jetstream::kv::Config {
                bucket: "todos".to_string(),
                history: 1,
                ..Default::default()
            })
            .await
            .unwrap();

        let mut watcher = kv.watch_all_from_revision(1).await.unwrap();
        while let Some(msg) = watcher.next().await {
            let msg = msg.unwrap();

            let todo: Todo = serde_json::from_slice(&msg.value).unwrap();
            todos.update(|todos| todos.push(todo));
        }
    });

    view! {
        <div>
            <h1>"Hello, world!"</h1>
            <For
                each=move || todos.get()
                key=|todo| todo.id
                children=move |todo| view! {
                    <div>
                        <input type="checkbox" checked=todo.completed />
                        <span>{todo.title}</span>
                    </div>
                }
            />
        </div>
    }
}
