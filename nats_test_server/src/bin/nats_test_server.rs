const USAGE: &str = "
Usage: nats_test_server [--host=<s>] [--port=<#>] [--hop-ports] [--bugginess=<s>]

Options:
    --host=<s>      Host to listen on [default: 0.0.0.0].
    --port=<n>      Port to listen on [default: 4222].
    --bugginess=<#> 1 in <bugginess> operations will fail [default: 200].
    --hop-ports     Hop ports to test client server learning [default: false].
";

#[derive(Clone, Debug)]
struct Args {
    port: u16,
    host: String,
    bugginess: u32,
    hop_ports: bool,
}

impl Default for Args {
    fn default() -> Args {
        Args {
            port: 4222,
            host: "0.0.0.0".into(),
            bugginess: 200,
            hop_ports: false,
        }
    }
}

fn parse<'a, I, T>(mut iter: I) -> T
where
    I: Iterator<Item = &'a str>,
    T: std::str::FromStr,
    <T as std::str::FromStr>::Err: std::fmt::Debug,
{
    iter.next().expect(USAGE).parse().expect(USAGE)
}

impl Args {
    fn parse() -> Args {
        let mut args = Args::default();
        for raw_arg in std::env::args().skip(1) {
            let mut splits = raw_arg.get(2..).expect(USAGE).split('=');
            match splits.next().expect(USAGE) {
                "host" => args.host = parse(&mut splits),
                "port" => args.port = parse(&mut splits),
                "bugginess" => args.bugginess = parse(&mut splits),
                "hop-ports" => args.hop_ports = true,
                other => panic!("unknown option: {}, {}", other, USAGE),
            }
        }
        args
    }
}

fn main() {
    use std::sync::{atomic::AtomicBool, Arc, Barrier};

    env_logger::from_env(env_logger::Env::default().default_filter_or("debug")).init();

    let args = Args::parse();
    log::info!("starting test server with args {:?}", &args);

    let barrier = Arc::new(Barrier::new(1));
    let shutdown = Arc::new(AtomicBool::new(false));
    let restart = Arc::new(AtomicBool::new(false));

    nats_test_server::nats_test_server(
        &args.host,
        args.port,
        barrier,
        shutdown,
        restart,
        args.bugginess,
        args.hop_ports,
    )
}
