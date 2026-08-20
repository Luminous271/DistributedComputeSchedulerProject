import argparse
from worker.worker import Worker

def main():
    parser = argparse.ArgumentParser() # pythons argument parser, automatically parser arumgnets when you run python script

    # add expected arguments, using this tag
    parser.add_argument(
        "--worker-id",
        required=True,
    )
    # get args from parser
    args = parser.parse_args()
    # using the args, make a worker and run it
    worker = Worker(args.worker_id)
    worker.run()


if __name__ == "__main__":
    main()