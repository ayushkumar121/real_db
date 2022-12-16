use std::process::exit;

fn main() {
    if let Err(e) = real_db::run() {
        println!("Error: {}", e);
        exit(1);
    };
}
