#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# drmr: A tool for submitting pipeline scripts to distributed resource
# managers.
#
# Copyright 2015 Stephen Parker
#
# Licensed under Version 3 of the GPL or any later version
#

import argparse
import datetime
import copy
import logging
import os
import sys
import textwrap

import drmr
import drmr.config
import drmr.exceptions
import drmr.script
import drmr.util


HELP = """

    Supported resource managers are:

{resource_managers}

    drmr will read configuration from your ~/.drmrc, which must be
    valid JSON. You can specify your resource manager and default
    values for any job parameters listed below.

    Directives
    ==========

    Your script can specify job control directives in special
    comments starting with "drmr:".

    # drmr:wait

      Drmr by default runs all the script's commands
      concurrently. The wait directive tells drmr to wait for
      any jobs started since the last wait directive, or the
      beginning of the script, to complete successfully.

    # drmr:label

      Labels let you selectively run sections of your script: you can
      restart from a label with --from-label, running everything after
      it, or just the commands before the label given with --to-label.

    # drmr:job

      You can customize the following job parameters:

{job_directives}

      Whatever you specify will apply to all jobs after the directive.

      To revert to default parameters, use:

      # drmr:job default

      To request 4 CPUs, 8GB of memory per processor, and a
      limit of 12 hours of execution time on one node:

      # drmr:job nodes=1 processors=4 processor_memory=8000 time_limit=12:00:00

    Example
    =======

    A complete example script follows:

    #!/bin/bash

    #
    # Example drmr script. It can be run as a normal shell script, or
    # submitted to a resource manager with the drmr command.
    #

    #
    # You can just write commands as you would in any script. Their output
    # will be captured in files by the resource manager.
    #
    echo thing1

    #
    # You can only use flow control within a command; drmr's parser is not
    # smart enough to deal with conditionals, or create jobs for each
    # iteration of a for loop, or anything like that.
    #
    # You can do this, but it will just all happen in a single job:
    #
    for i in $(seq 1 4); do echo thing${{i}}; done

    #
    # Comments are OK.
    #
    echo thing2  # even trailing comments


    #
    # Line continuations are OK.
    #
    echo thing1 \\
         thing2 \\
         thing3

    #
    # Pipes are OK.
    #
    echo funicular berry harvester | wc -w

    #
    # The drmr wait directive makes subsequent tasks depend on the
    # successful completion of all jobs since the last wait directive or
    # the start of the script.
    #

    # drmr:wait
    echo "And proud we are of all of them."

    #
    # You can specify job parameters:
    #

    # drmr:job nodes=1 processors=4 processor_memory=8000 time_limit=12:00:00
    echo "I got mine but I want more."

    #
    # And revert to the defaults defined by drmr or the resource manager.
    #

    # drmr:job default
    echo "This job feels so normal."

    # drmr:wait
    # drmr:job time_limit=00:15:00
    echo "All done!"

    # And finally, a job is automatically submitted to wait on all the
    # other jobs and report success or failure of the entire script.
    # Its job ID will be printed.

""".format(
    **{
        "job_directives": "\n".join(
            "      {}: {}".format(*i) for i in drmr.script.JOB_DIRECTIVES.items()
        ),
        "resource_managers": "\n".join(
            "      {}".format(name) for name in drmr.config.RESOURCE_MANAGERS.keys()
        ),
    }
)


def parse_arguments():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Submit a drmr script to a distributed resource manager.",
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
        "-f",
        "--from-label",
        dest="from_label",
        help="Ignore script lines before the given label.",
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
        "--start-held",
        dest="start_held",
        action="store_true",
        help="Submit a held job at the start of the pipeline, which must be released to start execution.",
    )
    parser.add_argument(
        "-t",
        "--to-label",
        dest="to_label",
        help="Ignore script lines after the given label.",
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


def make_wait_list_note(wait_list):
    note = ""
    if wait_list:
        note = "\n# wait list:\n#  " + "\n#  ".join(
            [jid + (job and " ({})".format(job) or "") for jid, job in wait_list]
        )
    return note


def create_job_data(
    template_data, job_name, command_text, wait_list=None, mail_on_error=False
):
    job_data = copy.deepcopy(template_data)
    job_data["job_name"] = job_name
    job_data["command"] = command_text
    if wait_list:
        job_data["notes"] = make_wait_list_note(wait_list)
        job_data["dependencies"] = {"ok": [wait_id for wait_id, wait_name in wait_list]}
    if mail_on_error:
        job_data["mail_events"] = ["FAIL"]

    return job_data


def create_job(
    resource_manager,
    template_data,
    job_name,
    command_text,
    wait_list=None,
    mail_on_error=False,
    start_held=False,
):
    job_data = create_job_data(
        template_data, job_name, command_text, wait_list, mail_on_error
    )
    job_file = resource_manager.write_job_file(job_data)
    return resource_manager.submit(job_file, start_held)


def create_jobs(
    resource_manager,
    template_data,
    script,
    wait_list=None,
    mail_at_finish=False,
    mail_on_error=False,
    from_label=None,
    to_label=None,
    start_held=False,
):
    if wait_list is None:
        wait_list = []

    logger = logging.getLogger("{}.{}".format(__name__, create_jobs.__name__))

    if from_label:
        logger.debug('Skipping everything before label "{}"'.format(from_label))

    if to_label:
        logger.debug('Skipping everything after label "{}"'.format(to_label))

    master_job_name = template_data["master_job_name"]
    if start_held:
        logger.debug("Submitting start hold job.")
        hold_job_name = master_job_name + ".start"
        hold_id = create_job(
            resource_manager,
            template_data,
            hold_job_name,
            """echo 'Job "{}" started.'""".format(master_job_name),
            wait_list,
            mail_on_error,
            start_held,
        )
        wait_list = [(hold_id, hold_job_name)]

    prereqs = wait_list[:]
    all_jobs = []
    job_directives = {}
    job_number = 0
    from_label_seen = False

    for line in script:
        directive, args = drmr.script.parse_directive(line)
        if directive:
            if directive == "job" and args:
                if args == "default":
                    job_directives = {}
                else:
                    job_directives.update(dict([a.split("=", 1) for a in args.split()]))
            elif directive == "label":
                if from_label is not None and from_label in args:
                    from_label_seen = True
                    logger.debug('From label "{}" seen. Starting.'.format(from_label))
                if to_label is not None and to_label in args:
                    logger.debug('To label "{}" seen. Stopping.'.format(to_label))
                    break
            elif directive == "wait" and wait_list:
                job_number += 1
                job_name = master_job_name + ".{}".format(job_number)
                job_data = drmr.util.merge_mappings(
                    template_data,
                    job_directives,
                    {
                        "job_name": job_name,
                        "notes": make_wait_list_note(wait_list),
                    },
                )
                job_id = resource_manager.submit_completion_jobs(
                    job_data, [wait_id for wait_id, wait_name in wait_list]
                )
                wait_list = [(job_id, job_name + ".success")]
                prereqs = wait_list[:]
        else:
            if from_label is not None and not from_label_seen:
                logger.debug("From label not yet seen, skipping line [{}]".format(line))
                continue

            job_number += 1
            job_name = master_job_name + ".{}".format(job_number)
            job_id = create_job(
                resource_manager,
                drmr.util.merge_mappings(template_data, job_directives),
                job_name,
                line,
                prereqs,
                mail_on_error,
            )
            wait_list.append((job_id, job_name))
            all_jobs.append(job_id)

    completion_data = drmr.util.merge_mappings(
        template_data,
        {"job_name": master_job_name, "notes": make_wait_list_note(wait_list)},
    )
    completion_job_id = resource_manager.submit_completion_jobs(
        completion_data, [w[0] for w in wait_list], mail_at_finish=mail_at_finish
    )

    cancel_data = drmr.util.merge_mappings(
        template_data, {"job_name": master_job_name + ".cancel"}
    )
    resource_manager.write_cancel_script(cancel_data, all_jobs + [completion_job_id])

    return all_jobs, completion_job_id


def main():
    args = parse_arguments()

    loglevel = args.debug and logging.DEBUG or logging.INFO
    logging.basicConfig(level=loglevel, format=drmr.script.LOGGING_FORMAT)

    logger = logging.getLogger(sys.argv[0])

    try:
        config = drmr.config.load_configuration(
            {"account": args.account, "destination": args.destination}
        )
        resource_manager = drmr.config.get_resource_manager(config["resource_manager"])
    except drmr.exceptions.ConfigurationError as e:
        print(e, file=sys.stderr)
        sys.exit(1)

    template_data = {
        "account": config["account"],
        "destination": config["destination"],
        "master_job_name": args.job_name or os.path.basename(args.input),
        "submission_directory": os.path.abspath(os.getcwd()),
        "timestamp": datetime.datetime.now().strftime("%Y%m%d%H%M%S"),
        "working_directory": os.path.abspath(os.getcwd()),
    }

    if args.input != "-" and not os.access(args.input, os.R_OK):
        print('Cannot read script file "{}"'.format(args.input), file=sys.stderr)
        sys.exit(1)

    input_file = args.input == "-" and sys.stdin or open(args.input)
    script = drmr.script.parse_script(input_file.read())

    wait_list = args.wait_list and args.wait_list.split(":") or []
    wait_list = [(job_id, "from command line") for job_id in wait_list]
    try:
        all_jobs, completion_job_id = create_jobs(
            resource_manager,
            template_data,
            script,
            wait_list,
            mail_at_finish=args.mail_at_finish,
            mail_on_error=args.mail_on_error,
            from_label=args.from_label,
            to_label=args.to_label,
            start_held=args.start_held,
        )
    except drmr.exceptions.SubmissionError as e:
        print("\nYour script could not be submitted.")
        print("Command '{}' returned {}.".format(" ".join(e.cmd), e.returncode))
        print("Command output was:\n\n{}\n".format(e.output))
        sys.exit(1)

    if all_jobs:
        print(completion_job_id)
    else:
        print("No jobs submitted. Check your script.")
        sys.exit(1)


if __name__ == "__main__":
    main()
