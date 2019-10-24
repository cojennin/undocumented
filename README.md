# undocumentedNY

This project is to support the DocumentedNY team. 

## Requirements for running locally
- Install [pipenv](https://github.com/pypa/pipenv) and install dependencies using pipenv
- Install Postgres (one option is [Postgres.app](https://postgresapp.com/))
- Install a client to access Postgres ([Postico](https://eggerapps.at/postico/) works well)

## Create a database in Postgres
Using Postico (or another client) create a database on your local machine

## Create config
Take `example_config.ini` and copy it over to `config.ini`. You need to subsitute your Postgres settings. 
Example `config.ini` (subsitute with your own values!):

```
[DB]
DB_USER=postgres_user
DB_HOST=127.0.0.1
DB_NAME=documented
DB_PASSWORD=postgres_password
```

## Run the application
- Run `pipenv shell`
- Then run `python load_data.py`
- You'll be prompted at stages for loading data