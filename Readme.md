# GTSA project

Gelf To Sentry Adapter is the simple solution to proxy gelf messages (messages for [Graylog](https://www.graylog.org/)) to [Sentry](https://sentry.io/)

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Prerequisites

You need to docker or cargo with rust

### Installing

You can install it with cargo 

```bash
cargo install --path .
gtsa
```

Or docker build

```bash
docker build -t gtsa .
docker run -p 8080:8080/udp --env SENTRY_DSN=dsn --name gtsa gtsa 
```

Now you can sent data on udp 

## Deployment

The simplest way to deploy that app is pull your docker container and run it with env:
```env
SENTRY_DSN=xxx
```

## Built With

* [actix](https://github.com/actix/actix) - The actor framework used
* [tokio](https://github.com/tokio-rs/tokio) - Async realisation
* [cargo](https://github.com/rust-lang/cargo) - Built with

## Contributing

I hope you know what you need to do.

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/Mnwa/gtsa/tags). 

## Authors

* **Mikhail Panfilov** - *Initial work* - [PurpleBooth](https://github.com/Mnwa)

See also the list of [contributors](https://github.com/Mnwa/gtsa/contributors) who participated in this project.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details