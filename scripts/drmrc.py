#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# drmrc: Generate a .drmrc for your local environment
#
# Copyright 2015 Stephen Parker
#
# Licensed under Version 3 of the GPL or any later version
#


import argparse
import json
import os
import sys

import drmr
import drmr.config

CONFIGURATION_PARAMETERS = ["account", "destination", "resource_manager"]


def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Generate a drmr configuration file for your local environment.",
    )

    parser.add_argument(
        "-a",
        "--account",
        dest="account",
        help="The account to which jobs will be charged by default.",
    )
    parser.add_argument(
        "-d",
        "--destination",
        dest="destination",
        help="The default queue/partition in which to run jobs.",
    )
    parser.add_argument(
        "-o",
        "--overwrite",
        dest="overwrite",
        action="store_true",
        help="Overwrite any existing configuration file.",
    )
    parser.add_argument(
        "-m",
        "--mail",
        dest="mail",
        help="The email address to which notifications will be sent by default.",
    )
    parser.add_argument(
        "-r",
        "--resource-manager",
        dest="resource_manager",
        help="If you have more than one resource manager available, you can specify it. Supported resource managers:\n{}".format(
            ", ".join(list(drmr.config.RESOURCE_MANAGERS.keys()))
        ),
    )

    args = vars(parser.parse_args())

    configuration_filename = os.path.expanduser("~/.drmrc")
    if os.path.exists(configuration_filename) and not args["overwrite"]:
        print(
            f"You already have a drmr configuration file ({configuration_filename}),"
            " which I won't overwrite unless you use the "
            "--overwrite option.",
            file=sys.stderr,
        )
        sys.exit(1)

    configuration = {}
    for key in CONFIGURATION_PARAMETERS:
        value = args.get(key)
        if value:
            configuration[key] = value

    resource_manager_name = configuration.get("resource_manager")
    if not resource_manager_name:
        available_resource_managers = drmr.config.get_available_resource_managers()
        if len(available_resource_managers) == 1:
            resource_manager_name = configuration["resource_manager"] = (
                available_resource_managers[0]
            )
        elif len(available_resource_managers) > 1:
            print(
                f"I found multiple resource managers: {available_resource_managers}. "
                f"Please run {sys.argv[0]} again, "
                "specifying one with the --resource-manager option.",
                file=sys.stderr,
            )
            sys.exit(1)
        else:
            print(
                """I could not find a resource manager on this system. Please
                check your environment and try again.""",
                file=sys.stderr,
            )
            sys.exit(1)
    else:
        if resource_manager_name not in list(drmr.config.RESOURCE_MANAGERS.keys()):
            print(
                (
                    """I don't recognize your resource manager, "{}". """
                    """The supported resource managers are: {}"""
                ).format(
                    resource_manager_name, list(drmr.config.RESOURCE_MANAGERS.keys())
                ),
                file=sys.stderr,
            )
            sys.exit(1)

    destination = configuration.get("destination")
    if destination:
        resource_manager = drmr.config.get_resource_manager(resource_manager_name)
        if not resource_manager.validate_destination(destination):
            print(
                """I couldn't verify that the destination "{}" exists. You might want to double-check it.""".format(
                    destination
                ),
                file=sys.stderr,
            )

    with open(configuration_filename, "w") as configuration_file:
        json.dump(configuration, configuration_file, indent=4, sort_keys=True)


if __name__ == "__main__":
    main()
