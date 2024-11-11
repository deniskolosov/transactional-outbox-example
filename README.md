[![CI](https://github.com/deniskolosov/transactional-outbox-example/actions/workflows/django.yml/badge.svg)](https://github.com/deniskolosov/transactional-outbox-example/actions/workflows/django.yml)

This is example of application of Transactional outbox pattern.

## Problem:
The application sends event logs for analysis using a column-based Clickhouse database and a One Big Table (OBT) architecture with specific columns. Current challenges include missed logs due to lack of transactionality, poor UX from network errors, and inefficiency with large numbers of small inserts. We aim to create a new write mechanism to address these challenges.

## Solution:
We can apply transactional outbox pattern
![image](https://github.com/user-attachments/assets/47ffa7a3-565e-416f-97eb-c212dd40deaf)


1. User actions translate into operations within Django.
2. CreateUser & Log to the EventOutbox table in a transaction.
3. Background Worker retrieves unprocessed outbox entries.
4. Worker performs batch insert into Clickhouse.
5. Worker marks outbox entries as processed.

## Installation

Put a `.env` file into the `src/core` directory. You can start with a template file:

```
cp src/core/.env.ci src/core/.env
```

Run the containers with
```
make run
```

and then run the installation script with:

```
make install
```

## Tests

`make test`

## Linter

`make lint`
