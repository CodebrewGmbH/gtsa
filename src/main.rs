use actix::System;
use std::env;
use std::fs;
use std::io::{Error, ErrorKind};

mod gelf;
mod sentry;

use crate::gelf::gelf_message_processor::GelfPrinterActor;
use crate::sentry::sentry_processor::SentryProcessorActor;
use gelf::gelf_reader::GelfReaderActor;
use gelf::tcp_acceptor;
use gelf::udp_acceptor;
use gelf::unpacking::UnPackActor;
use std::sync::Arc;

fn get_sentry_dsn() -> Result<String, Error> {
    if env::var("SENTRY_DSN").is_ok() && env::var("SENTRY_DSN_FILE").is_ok() {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "Only one SENTRY_DSN or SENTRY_SDN_FILE should be specified!",
        ));
    }

    let dsn =
        match env::var("SENTRY_DSN") {
            Ok(s) => s,
            Err(_) => match env::var("SENTRY_DSN_FILE") {
                Ok(f) => match fs::read_to_string(f) {
                    Ok(s) => s,
                    Err(_) => {
                        return Err(Error::new(
                            ErrorKind::InvalidInput,
                            "Defined SENTRY_DSN_FILE not found",
                        ))
                    }
                },
                Err(_) => return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "SENTRY_DSN and SENTRY_DSN_FILE not defined you must pass one of both as env",
                )),
            },
        };

    Ok(dsn)
}

fn main() -> Result<(), Error> {
    let dsn = get_sentry_dsn()?;
    let udp_addr = env::var("UDP_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
    let tcp_addr = env::var("TCP_ADDR").unwrap_or_else(|_| "0.0.0.0:8081".to_string());
    let system_name = env::var("SYSTEM").unwrap_or_else(|_| "Gelf Mover".to_string());

    let reader_threads: usize = env::var("READER_THREADS")
        .unwrap_or_else(|_| "1".to_string())
        .parse()
        .unwrap();
    let unpacker_threads: usize = env::var("UNPACKER_THREADS")
        .unwrap_or_else(|_| "1".to_string())
        .parse()
        .unwrap();
    let max_parallel_chunks: usize = std::env::var("MAX_PARALLEL_CHUNKS")
        .unwrap_or_else(|_| "500".to_string())
        .parse()
        .unwrap();

    let system = System::new(system_name);
    let gelf_reader = Arc::new(GelfReaderActor::new(reader_threads));
    let gelf_unpacker = Arc::new(UnPackActor::new(unpacker_threads));
    let gelf_sentry_processor = Arc::new(SentryProcessorActor::new(&dsn, reader_threads));
    let _gelf_printer = GelfPrinterActor::new();
    actix::spawn(udp_acceptor::new_udp_acceptor(
        udp_addr,
        Arc::clone(&gelf_sentry_processor),
        Arc::clone(&gelf_reader),
        gelf_unpacker,
        max_parallel_chunks,
    ));
    actix::spawn(tcp_acceptor::new_tcp_acceptor(
        tcp_addr,
        Arc::clone(&gelf_sentry_processor),
        Arc::clone(&gelf_reader),
    ));
    system.run().unwrap();
    Ok(())
}

#[cfg(test)]
mod tests {

    use std::env;
    use std::io::Write;
    use tempfile::NamedTempFile;

    // make sure to run tests in one thread only, otherwise it is possible we get race conditions for
    // environment variables set/unset and assigning both file tests the same filename
    use serial_test::serial;

    #[test]
    #[serial]
    fn test_with_dsn() {
        env::set_var("SENTRY_DSN", "http://12354@example.com/5");
        env::remove_var("SENTRY_DSN_FILE");
        let res = super::get_sentry_dsn();
        assert_eq!(res.unwrap(), "http://12354@example.com/5");
    }

    #[test]
    #[serial]
    fn test_with_dsn_file() {
        let mut file = NamedTempFile::new().unwrap();
        let _ = write!(file, "http://12354@example.com/5");
        env::set_var("SENTRY_DSN_FILE", file.path());
        env::remove_var("SENTRY_DSN");
        let res = super::get_sentry_dsn();
        assert_eq!(res.unwrap(), "http://12354@example.com/5");
        file.close().unwrap();
    }

    #[test]
    #[serial]
    fn test_with_dsn_file_not_exists() {
        let file = NamedTempFile::new().unwrap();
        env::set_var("SENTRY_DSN_FILE", file.path());
        env::remove_var("SENTRY_DSN");
        file.close().unwrap();
        let res = super::get_sentry_dsn();
        assert!(res.is_err());
    }

    #[test]
    #[serial]
    fn test_without_dsn() {
        env::remove_var("SENTRY_DSN");
        env::remove_var("SENTRY_DSN_FILE");
        let res = super::get_sentry_dsn();
        assert!(res.is_err())
    }

    #[test]
    #[serial]
    fn test_with_both_dsn() {
        env::set_var("SENTRY_DSN", "http://12354@example.com/5");
        env::set_var("SENTRY_DSN_FILE", "/tmp/test");
        let res = super::get_sentry_dsn();
        assert!(res.is_err())
    }
}
