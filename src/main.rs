use real_db;
use std::process::exit;

fn main() {
    match real_db::run() {
        Err(e) => {
            println!("Error: {}", e);
            exit(1);
        }
        _ => {}
    };
}
