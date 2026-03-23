use chrono::NaiveDateTime;
use leptos::{ev, prelude::*, task::spawn_local};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Clone, PartialEq, Eq)]
struct Todo {
    id: String,
    title: RwSignal<String>,
    completed: RwSignal<bool>,
    date: NaiveDateTime,
}

#[derive(Clone)]
enum TodoCommand {
    Upsert(Todo),
    Delete(String),
}

// TODO: take a look how we can ignore untracked fields.
impl Serialize for Todo {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("Todo", 4)?;
        state.serialize_field("id", &self.id)?;
        state.serialize_field("title", &self.title.get_untracked())?;
        state.serialize_field("completed", &self.completed.get_untracked())?;
        state.serialize_field("date", &self.date)?;
        state.end()
    }
}

impl std::cmp::PartialOrd for Todo {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl std::cmp::Ord for Todo {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if self.id == other.id {
            std::cmp::Ordering::Equal
        } else {
            self.date.cmp(&other.date).then(self.id.cmp(&other.id))
        }
    }
}

#[component]
pub fn App() -> impl IntoView {
    use async_nats::jetstream;
    use futures::{channel::mpsc, StreamExt};

    let (tx, mut rx) = mpsc::channel::<TodoCommand>(1);
    let todos = RwSignal::new(Vec::<Todo>::new());
    let title = RwSignal::new(String::new());

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
        let pull = async move {
            while let Some(msg) = watcher.next().await {
                let msg = msg.unwrap();

                match msg.operation {
                    jetstream::kv::Operation::Put => {
                        let todo: Todo = serde_json::from_slice(&msg.value).unwrap();
                        todos.update(|todos| match todos.binary_search(&todo) {
                            Ok(pos) => {
                                todos[pos].id = todo.id;
                                todos[pos].title.set(todo.title.get());
                                todos[pos].completed.set(todo.completed.get());
                                todos[pos].date = todo.date;
                            }
                            Err(pos) => todos.insert(pos, todo),
                        });
                    }
                    jetstream::kv::Operation::Delete | jetstream::kv::Operation::Purge => {
                        todos.update(|todos| {
                            if let Ok(pos) = todos.binary_search_by(|t| t.id.cmp(&msg.key)) {
                                todos.remove(pos);
                            }
                        });
                    }
                }
            }
        };

        let push = async move {
            while let Some(command) = rx.next().await {
                match command {
                    TodoCommand::Upsert(todo) => {
                        let value = serde_json::to_vec(&todo).unwrap();
                        kv.put(todo.id, value.into()).await.unwrap();
                        title.set(String::new());
                    }
                    TodoCommand::Delete(id) => {
                        kv.delete(id).await.unwrap();
                    }
                }
            }
        };

        futures::join!(push, pull);
    });

    let total = Signal::derive(move || todos.with(|todos| todos.len()));
    let completed = Signal::derive(move || {
        todos.with(|todos| todos.iter().filter(|t| t.completed.get()).count())
    });
    let remaining = Signal::derive(move || total.get().saturating_sub(completed.get()));

    let mut add_tx = tx.clone();
    let add_todo = move |ev: ev::SubmitEvent| {
        ev.prevent_default();

        let raw_title = title.get_untracked();
        let trimmed_title = raw_title.trim();
        if trimmed_title.is_empty() {
            return;
        }

        let todo = Todo {
            id: nuid::next().to_string(),
            title: RwSignal::new(trimmed_title.to_string()),
            completed: RwSignal::new(false),
            date: chrono::Utc::now().naive_utc(),
        };

        let _ = add_tx.try_send(TodoCommand::Upsert(todo));
    };

    let mut toggle_tx = tx.clone();
    let toggle_todo = move |todo: &Todo, completed: bool| {
        let _ = toggle_tx.try_send(TodoCommand::Upsert(Todo {
            id: todo.id.clone(),
            title: todo.title.clone(),
            completed: RwSignal::new(completed),
            date: todo.date,
        }));
    };

    let mut delete_tx = tx.clone();
    let delete_todo = move |todo: &Todo| {
        let _ = delete_tx.try_send(TodoCommand::Delete(todo.id.clone()));
    };

    view! {
        <main class="min-h-screen bg-slate-950 px-4 py-10 text-slate-100">
            <section class="mx-auto flex max-w-3xl flex-col gap-8">
                <div class="space-y-3">
                    <span class="inline-flex rounded-full border border-cyan-400/30 bg-cyan-400/10 px-3 py-1 text-xs font-semibold uppercase tracking-[0.24em] text-cyan-200">
                        "NATS + JetStream + Leptos"
                    </span>
                    <div class="space-y-2">
                        <h1 class="text-4xl font-black tracking-tight text-white sm:text-5xl">
                            "Shared Todo Board"
                        </h1>
                        <p class="max-w-2xl text-sm leading-6 text-slate-300 sm:text-base">
                            "Create a todo in the browser and it is pushed into a JetStream KV bucket over WebSockets."
                        </p>
                    </div>
                </div>

                <div class="grid gap-4 sm:grid-cols-3">
                    <div class="rounded-2xl border border-white/10 bg-white/5 p-4 backdrop-blur">
                        <div class="text-xs uppercase tracking-[0.24em] text-slate-400">"Total"</div>
                        <div class="mt-2 text-3xl font-bold text-white">{total}</div>
                    </div>
                    <div class="rounded-2xl border border-emerald-400/20 bg-emerald-400/10 p-4 backdrop-blur">
                        <div class="text-xs uppercase tracking-[0.24em] text-emerald-200/80">"Completed"</div>
                        <div class="mt-2 text-3xl font-bold text-emerald-100">{completed}</div>
                    </div>
                    <div class="rounded-2xl border border-amber-400/20 bg-amber-400/10 p-4 backdrop-blur">
                        <div class="text-xs uppercase tracking-[0.24em] text-amber-200/80">"Remaining"</div>
                        <div class="mt-2 text-3xl font-bold text-amber-100">{remaining}</div>
                    </div>
                </div>

                <form class="rounded-3xl border border-white/10 bg-white/5 p-4 shadow-2xl shadow-slate-950/30 backdrop-blur sm:p-6" on:submit=add_todo>
                    <div class="flex flex-col gap-3 sm:flex-row">
                        <input
                            class="h-12 flex-1 rounded-2xl border border-white/10 bg-slate-900/80 px-4 text-base text-white outline-none transition placeholder:text-slate-500 focus:border-cyan-300/70"
                            type="text"
                            placeholder="Add a todo and press Enter"
                            prop:value=move || title.get()
                            on:input=move |ev| title.set(event_target_value(&ev))
                        />
                        <button
                            class="h-12 rounded-2xl bg-cyan-300 px-5 font-semibold text-slate-950 transition hover:bg-cyan-200 focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-cyan-200"
                            type="submit"
                        >
                            "Add Todo"
                        </button>
                    </div>
                </form>

                <div class="rounded-3xl border border-white/10 bg-slate-900/70 p-4 shadow-2xl shadow-slate-950/30 sm:p-6">
                    <div class="mb-4 flex items-center justify-between">
                        <h2 class="text-lg font-semibold text-white">"Todos"</h2>
                        <span class="text-sm text-slate-400">
                            {move || if total.get() == 0 { "Waiting for updates".to_string() } else { format!("{} item(s)", total.get()) }}
                        </span>
                    </div>

                    <Show
                        when=move || { total.get() > 0 }
                        fallback=move || view! {
                            <div class="rounded-2xl border border-dashed border-white/10 bg-white/[0.03] px-5 py-10 text-center text-sm text-slate-400">
                                "No todos yet. Add one above to publish it into the KV bucket."
                            </div>
                        }
                    >
                        <div class="space-y-3">
                            <For
                                each=move || todos.get()
                                key=|todo| todo.id.clone()
                                children={
                                    let toggle_todo = toggle_todo.clone();
                                    let delete_todo = delete_todo.clone();
                                    move |todo| {
                                    view! {
                                        <label class="flex items-center gap-4 rounded-2xl border border-white/10 bg-white/[0.04] px-4 py-3 transition hover:border-cyan-300/30 hover:bg-white/[0.06]">
                                            <input
                                                class="h-5 w-5 rounded border-white/20 bg-slate-950 text-cyan-300 focus:ring-cyan-300"
                                                type="checkbox"
                                                checked=todo.completed
                                                on:click={
                                                    let mut toggle_todo = toggle_todo.clone();
                                                    let todo = todo.clone();
                                                    move |ev| {
                                                        ev.stop_propagation();
                                                        toggle_todo(&todo, event_target_checked(&ev));
                                                    }
                                                }
                                            />
                                            <div class="min-w-0 flex-1">
                                                <p class=move || {
                                                    if todo.completed.get() {
                                                        "truncate text-sm font-medium text-slate-500 line-through"
                                                    } else {
                                                        "truncate text-sm font-medium text-white"
                                                    }
                                                }>{todo.title.get()}</p>
                                                <p class="mt-1 text-xs uppercase tracking-[0.2em] text-slate-500">
                                                    {format!("#{}", todo.id.clone())}
                                                </p>
                                            </div>
                                            <button
                                                class="shrink-0 rounded-xl border border-rose-400/20 bg-rose-400/10 px-3 py-2 text-xs font-semibold uppercase tracking-[0.18em] text-rose-200 transition hover:border-rose-300/40 hover:bg-rose-400/20"
                                                type="button"
                                                on:click={
                                                    let mut delete_todo = delete_todo.clone();
                                                    let todo = todo.clone();
                                                    move |ev| {
                                                        ev.stop_propagation();
                                                        delete_todo(&todo);
                                                    }
                                                }
                                            >
                                                "Delete"
                                            </button>
                                        </label>
                                    }
                                }}
                            />
                        </div>
                    </Show>
                </div>
            </section>
        </main>
    }
}
