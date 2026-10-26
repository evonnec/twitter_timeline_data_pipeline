# Twitter Timeline Data Pipeline

A serverless data pipeline on AWS. For a given X (Twitter) account, it collects each Tweet's metadata and any attached media, stores the metadata in DynamoDB and the media in S3, and uses SQS to decouple fetching from storage.

## Goals

The pipeline is meant to:

- Authenticate to the X API v2 with a bearer token and collect a user's timeline Tweets, each Tweet's metadata (`created_at`, `text`, `referenced_tweets`, and more), and any attached media.
- Run on Lambda, SQS, S3 and DynamoDB: a scheduled Lambda fetches Tweets and enqueues them, and a queue-triggered Lambda stores metadata in DynamoDB and media in S3.
- Be efficient, reliable and scalable.
- Keep the API token in Secrets Manager.
- Deploy from a YAML CloudFormation template, with a separate Python module for each Lambda function.

## Current status

This repository holds an early proof of concept that covers part of these goals. See `INSTRUCTIONS.md` for what works and how to deploy it.
