#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# drmrarray: A tool for submitting pipeline scripts to distributed
# resource managers in array jobs.
#
# Copyright 2015 Stephen Parker
#
# Licensed under Version 3 of the GPL or any later version
#

# Copyright 2023 Vivek Rai


import argparse
import datetime
import logging
import os
import sys
import textwrap

import drmr
import drmr.config
import drmr.exceptions
import drmr.script


HELP = """

    Supported resource managers are:

{resource_managers}

    drmrarray will read configuration from your ~/.drmrc, which must be valid
    JSON. You can specify your resource manager and default values for any job
    parameters listed below.

    Directives
    ==========

    Your script can specify job parameters in special comments starting
    with "drmr:job".

    # drmr:job

      You can customize the following job parameters:

{job_directives}

      Whatever you specify will apply to all jobs after the directive.

      To revert to default parameters, use:

      # drmr:job default

      To request 4 CPUs, 8GB of memory per processor, and a
      limit of 12 hours of execution time on one node:

      # drmr:job nodes=1 processors=4 processor_memory=8000 time_limit=12:00:00

""".format(
    **{
        "job_directives": "\n".join(
            "      {}: {}".format(*i) for i in list(drmr.script.JOB_DIRECTIVES.items())
        ),
        "resource_managers": "\n".join(
            "      {}".format(name)
            for name in list(drmr.config.RESOURCE_MANAGERS.keys())
        ),
    }
)


def parse_slot_limit(slot_limit):
    """Makes sure slot limit is 'all' or a valid integer."""
    try:
        slot_limit = int(slot_limit)
    except ValueError:
        try:
            if slot_limit.lower() == "all":
                slot_limit = "all"
        except AttributeError:  # not a string
            slot_limit = 100

    return slot_limit


def parse_arguments():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Submit a drmr script to a distributed resource manager as a job array.",
        epilog=textwrap.dedent(HELP),
    )

    parser.add_argument(
        "-a", "--account", dest="account", help="The account to be billed for the jobs."
    )
    parser.add_argument(
        "-d",
        "--destination",
        dest="destination",
        help="The queue/partition in which to run the jobs.",
    )
    parser.add_argument(
        "--debug",
        dest="debug",
        action="store_true",
        help="Turn on debug-level logging.",
    )
    parser.add_argument("-j", "--job-name", dest="job_name", help="The job name.")
    parser.add_argument(
        "--mail-at-start",
        dest="mail_at_start",
        action="store_true",
        help="Send mail when jobs are started.",
    )
    parser.add_argument(
        "--mail-at-finish",
        dest="mail_at_finish",
        action="store_true",
        help="Send mail when all jobs are finished.",
    )
    parser.add_argument(
        "--mail-on-error",
        dest="mail_on_error",
        action="store_true",
        help="Send mail if any job fails.",
    )
    parser.add_argument(
        '-m',
        '--mail',
        dest='mail',
        help='The email address to which notifications will be sent if required.',
    )
    parser.add_argument(
        "-s",
        "--slot-limit",
        type=parse_slot_limit,
        default="all",
        dest="slot_limit",
        help="The number of jobs that will be run concurrently when the job is started, or 'all' (the default).",
    )
    parser.add_argument(
        "-w",
        "--wait-list",
        dest="wait_list",
        help="A colon-separated list of job IDs that must complete before any of this script's jobs are started.",
    )
    parser.add_argument(
        "input", help='The file containing commands to submit. Use "-" for stdin.'
    )

    return parser.parse_args()


def create_jobs(resource_manager, template_data, script, wait_list=None):
    if wait_list is None:
        wait_list = []

    job_directives = {}

    job_data = template_data.copy()
    job_data["job_name"] = job_data["master_job_name"]

    commands = []
    for i, line in enumerate(script, 1):
        directive, args = drmr.script.parse_directive(line)
        if directive:
            if commands:
                raise SyntaxError(
                    "Any drmr directives must appear before the first command in the script"
                )
            if directive == "job" and args:
                job_directives = dict([a.split("=", 1) for a in args.split()])
                job_data.update(job_directives)
        else:
            command_data = {"command": line, "index": len(commands) + 1}
            commands.append(resource_manager.make_array_command(command_data))

    command_count = max(len(commands), 1)
    slot_limit = job_data.get("slot_limit", "all")

    job_data.update(
        {
            "array_controls": {
                "array_index_min": 1,
                "array_index_max": command_count,
                "array_concurrent_jobs": slot_limit == "all"
                and command_count
                or slot_limit,
            },
            "command": "\n".join(commands),
        }
    )

    job_file = resource_manager.write_job_file(job_data)
    job_id = resource_manager.submit(job_file)

    return job_id


def main():
    args = parse_arguments()

    loglevel = args.debug and logging.DEBUG or logging.INFO
    logging.basicConfig(level=loglevel, format=drmr.script.LOGGING_FORMAT)

    try:
        config = drmr.config.load_configuration(
            {"account": args.account, "destination": args.destination, "mail": args.mail}
        )
        resource_manager = drmr.config.get_resource_manager(config["resource_manager"])
    except drmr.exceptions.ConfigurationError as e:
        print(e, file=sys.stderr)
        sys.exit(1)

    template_data = {
        "account": config["account"],
        "destination": config["destination"],
        "email": config["mail"],
        "master_job_name": args.job_name or os.path.basename(args.input),
        "slot_limit": args.slot_limit,
        "submission_directory": drmr.util.absjoin(os.getcwd()),
        "timestamp": datetime.datetime.now().strftime("%Y%m%d%H%M%S"),
        "working_directory": os.path.abspath(os.getcwd()),
        "mail_events": []
    }

    # Setup emailing events
    if args.mail_on_error:
        template_data["mail_events"].append("FAIL")

    if args.mail_at_finish:
        template_data["mail_events"].append("END")

    if args.mail_at_start:
        template_data["mail_events"].append("BEGIN")

    if args.input != "-" and not os.access(args.input, os.R_OK):
        print('Cannot read script file "{}"'.format(args.input), file=sys.stderr)
        sys.exit(1)

    input_file = args.input == "-" and sys.stdin or open(args.input)
    script = [
        line
        for line in drmr.script.parse_script(input_file.read())
        if not drmr.script.is_boring(line)
    ]

    wait_list = args.wait_list and args.wait_list.split(":") or []

    try:
        completion_job_id = create_jobs(
            resource_manager, template_data, script, wait_list
        )
        print(completion_job_id)
    except drmr.exceptions.SubmissionError as e:
        print("\nYour script could not be submitted.")
        print("Command '%s' returned %s." % (" ".join(e.cmd), e.returncode))
        print("Command output was:\n\n%s\n" % e.output)
        sys.exit(1)


if __name__ == "__main__":
    main()
