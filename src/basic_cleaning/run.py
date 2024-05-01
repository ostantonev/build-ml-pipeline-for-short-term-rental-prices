#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    artifact_local_path = run.use_artifact(args.input_artifact).file()

    ######################
    # YOUR CODE HERE     #
    ######################
    # steps from EDA.ipynb
    df = pd.read_csv(artifact_local_path)
    min_price = args.min_price
    max_price = args.max_price
    idx = df['price'].between(min_price, max_price)
    cleaned = df[idx].copy()
    # Convert last_review to datetime
    cleaned['last_review'] = pd.to_datetime(df['last_review'])
    logger.info("cleaning steps from EDA.ipynb finished")

    # save
    logger.info(f"exporting cleaned data to {args.output_artifact}")
    cleaned.to_csv(args.output_artifact, index=False)

    # wandb stuff
    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    artifact.add_file(args.output_artifact)

    run.log_artifact(artifact)

    # Wait for upload completion
    artifact.wait()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help='artefact_before cleaning',
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help='cleaned artefact',
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help='W&B artifact output type',
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help='description of output artefact',
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help='min price to consider',
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help='max price to consider',
        required=True
    )


    args = parser.parse_args()

    go(args)
