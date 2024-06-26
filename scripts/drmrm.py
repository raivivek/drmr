#!/usr/bin/env python

#
# drmrm: remove jobs from a distributed resource manager
#


import argparse
import getpass
import logging
import sys

import drmr
import drmr.config
import drmr.exceptions
import drmr.script


def parse_arguments():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Remove jobs from a distributed resource manager.",
    )

    parser.add_argument(
        "--debug",
        dest="debug",
        action="store_true",
        help="Turn on debug-level logging.",
    )
    parser.add_argument(
        "-n",
        "--dry-run",
        action="store_true",
        help="Just print jobs that would be removed, without actually removing them.",
    )
    parser.add_argument(
        "-j", "--job-name", help="Remove only jobs whose names contain this string."
    )
    parser.add_argument(
        "-u",
        "--user",
        default=getpass.getuser(),
        help="Remove only jobs belonging to this user.",
    )
    parser.add_argument(
        "job_ids", nargs="*", metavar="job_id", help="A job ID to remove."
    )

    return parser.parse_args()


def main():
    args = parse_arguments()

    loglevel = args.debug and logging.DEBUG or logging.INFO
    logging.basicConfig(level=loglevel, format=drmr.script.LOGGING_FORMAT)

    try:
        config = drmr.config.load_configuration()
        resource_manager = drmr.config.get_resource_manager(config["resource_manager"])
        resource_manager.delete_jobs(
            args.job_ids, args.job_name, args.user, args.dry_run
        )
    except drmr.exceptions.ConfigurationError as e:
        print(e, file=sys.stderr)
        sys.exit(1)
    except drmr.exceptions.DeletionError as e:
        print(e, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
