#
# drmr: A tool for submitting pipeline scripts to distributed resource
# managers.
#
# Copyright 2023 Vivek Rai 
#
# Licensed under Version 3 of the GPL or any later version
#

from __future__ import print_function

import collections
import logging
import os
import subprocess
import textwrap

import lxml.objectify

import drmr
import drmr.drm.base
import drmr.util

class UGE(drmr.drm.base.DistributedResourceManager):
    name = 'UGE'

    default_job_template = textwrap.dedent(
        """
        #!/bin/bash

        #### UGE preamble
        
        #$ -N {{job_name}}
        {% if processors %}
        #$ -pe smp {{processors}}
        {% endif %}
        {% if time_limit %}
        #$ -l h_rt={{time_limit}}
        {% endif %}
        {% if memory %}
        #$ -l h_vmem={{memory|default('6G')}}
        {% endif %}
        {% if account %}
        #$ -P {{account}}
        {% endif %}
        {% if destination %}
        #$ -q {{destination}}
        {% endif %}
        {% if mail_event_string %}
        #$ -m {{mail_event_string}}
        {% endif %}
        {% if email %}
        #$ -M {{email}}
        {% endif %}
        {% if working_directory %}
        #$ -wd "{{working_directory}}"
        {% endif %}

        {% if dependencies %}
        #$ -hold_jid {{resource_manager.make_dependency_string(dependencies)}}
        {% endif %}
        #$ -o "{{control_directory}}/{{job_name}}_$SGE_TASK_ID.out"
        #$ -e "{{control_directory}}/{{job_name}}_$SGE_TASK_ID.err"
        
        {% if array_controls %}
        #$ -t {{array_controls['array_index_min']|default(1)}}-{{array_controls['array_index_max']|default(1)}}{% if array_controls['array_concurrent_jobs'] %}:{{array_controls['array_concurrent_jobs']}}{% endif %}
        {% endif %}

        {% if raw_preamble %}
        {{raw_preamble}}
        {% endif %}

        #### End UGE preamble

        {% if notes %}
        #### Notes
        {{notes}}

        {% endif %}
        {% if environment_setup %}
        #### Environment setup
        {% for line in environment_setup %}
        {{line}}
        {% endfor %}

        {% endif %}
        
        #### Commands
        
        {{command}}
        """
    ).lstrip()

    default_array_command_template = textwrap.dedent(
        """
        if [ "$SGE_TASK_ID" = "{{index}}" ]; then
            {{command}}
        fi
        """
    )

    job_dependency_states = [
        'any',
        'notok',
        'ok',
    ]

    job_state_map = {
        'any': '',
        'notok': '',
        'ok': '',
        'start': '',
    }

    mail_event_map = {
        'BEGIN': 'b',
        'END': 'e',
        'FAIL': 'a',
    }

    def delete_jobs(self, job_ids=None, job_name=None, job_owner=None, dry_run=False):
        logger = self.get_method_logger()

        if job_ids is None:
            job_ids = []

        targets = set(job_ids)
        targets.update(self.get_active_job_ids(job_ids, job_name, job_owner))

        if targets:
            if dry_run:
                logger.info(self.explain_job_deletion(targets, job_name, job_owner, dry_run))
            else:
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(self.explain_job_deletion(targets, job_name, job_owner, dry_run))
                command = ['qdel'] + list(targets)
                try:
                    subprocess.check_call(command)
                except subprocess.CalledProcessError as e:
                    raise drmr.exceptions.DeletionError(e.returncode, e.cmd, e.output, targets)

    def get_active_job_ids(self, job_ids=None, job_name=None, job_owner=None):
        logger = self.get_method_logger()

        if job_ids is None:
            job_ids = []

        jobs = set([])

        command = [
            'qstat',
            '-u', os.getenv('USER'),
            '-xml'
        ]

        qstat = lxml.objectify.fromstring(self.capture_process_output(command))
        job_list_items = qstat.job_info.job_list
        for job in job_list_items:
            if job.state in ['E']:
                continue

            if job_name and job_name not in job.JB_name:
                continue

            if job_ids and job.JB_job_number.text not in job_ids:
                continue

            owner = job.JB_owner.text
            if job_owner and job_owner != owner:
                continue

            jobs.add(job.JB_job_number.text)

        if jobs:
            logger.debug('Found {} active jobs'.format(len(jobs)))
        else:
            logger.debug('No active jobs found.')

        return jobs


    def make_dependency_string(self, dependencies):
        dependency_string = ''
        if dependencies:
            dependency_list = []
            if not isinstance(dependencies, collections.Mapping):
                raise ValueError('Job data does not contain a map under the "dependencies" key.')
            for state, job_ids in dependencies.items():
                if state not in self.job_dependency_states:
                    raise ValueError('Unsupported dependency state: %s' % state)

                dependency_list.append(','.join(str(job_id) for job_id in job_ids))
            dependency_string = ','.join(dependency_list)

        return dependency_string


    def is_installed(self):
        output = ''
        try:
            output = self.capture_process_output(['qstat', '-help'])
        except:
            pass

        return 'UGE' in output

    def write_cancel_script(self, job_data, job_ids):
        logger = self.get_method_logger()
        logger.debug('Writing canceller script for {}'.format(job_data))

        self.set_control_directory(job_data)
        filename = drmr.util.absjoin(job_data['control_directory'], job_data['job_name'])
        if not os.path.exists(job_data['control_directory']):
            os.makedirs(job_data['control_directory'])
        with open(filename, 'w') as canceller:
            canceller.write('#!/bin/sh\n\n')
            for job in job_ids:
                canceller.write('qdel {}\n'.format(job))
            os.chmod(filename, 0o755)

    def set_mail_event_string(self, job_data):
        if job_data.get('mail_events'):
            job_data['mail_event_string'] = ','.join(
                sorted(
                    self.mail_event_map[event] for event in job_data['mail_events']
                )
            )

    def submit(self, job_filename, hold=False):
        if not self.is_installed():
            raise drmr.exceptions.ConfigurationError('{} is not installed or not usable.'.format(self.name))

        try:
            command = ['qsub', job_filename]
            if hold:
                command.append('-h')

            job_id_output = self.capture_process_output(command)
            job_id = job_id_output.strip().split()[2]
            return job_id
        except subprocess.CalledProcessError as e:
            raise drmr.exceptions.SubmissionError(e.returncode, e.cmd, e.output)


    def validate_destination(self, destination):
        if not self.is_installed():
            raise drmr.exceptions.ConfigurationError('{} is not installed or not usable.'.format(self.name))

        valid = False
        try:
            command = ['qconf', '-sp', destination]
            status = self.capture_process_output(command)
            if 'pe_name' in status and 'slots' in status:
                valid = True
        except:
            pass

        return valid